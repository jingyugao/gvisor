// Copyright 2026 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgalloc

import (
	"bytes"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/context"
	"gvisor.dev/gvisor/pkg/hostarch"
	"gvisor.dev/gvisor/pkg/memutil"
	"gvisor.dev/gvisor/pkg/safemem"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
	"gvisor.dev/gvisor/pkg/sentry/state/stateio"
)

const foregroundWaitTestTimeout = 5 * time.Second

func newForegroundWaitTestMemoryFile(t *testing.T) *MemoryFile {
	t.Helper()
	fd, err := memutil.CreateMemFD(t.Name(), 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), t.Name())
	mf, err := NewMemoryFile(file, MemoryFileOpts{
		DisableIMAWorkAround:    true,
		DisableMemoryAccounting: true,
	})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	t.Cleanup(mf.Destroy)
	return mf
}

// blockedForegroundPageReader holds the first page read until release is closed. Reads of
// other pages can complete independently using the IOReader's other worker.
type blockedForegroundPageReader struct {
	*bytes.Reader
	started chan struct{}
	release chan struct{}
}

func (r *blockedForegroundPageReader) ReadAt(dst []byte, off int64) (int, error) {
	if off == 0 {
		close(r.started)
		<-r.release
	}
	return r.Reader.ReadAt(dst, off)
}

// foregroundObservedPageReader records how many operations are in flight at the first
// Wait call. Its methods are called serially, as required by AsyncReader.
type foregroundObservedPageReader struct {
	stateio.AsyncReader
	inflight  int
	observed  bool
	firstWait chan int
}

func (r *foregroundObservedPageReader) AddRead(id int, off int64, df stateio.DestinationFile, fr memmap.FileRange, dst []byte) {
	r.inflight++
	r.AsyncReader.AddRead(id, off, df, fr, dst)
}

func (r *foregroundObservedPageReader) AddReadv(id int, off int64, total uint64, df stateio.DestinationFile, frs []memmap.FileRange, dst []unix.Iovec) {
	r.inflight++
	r.AsyncReader.AddReadv(id, off, total, df, frs, dst)
}

func (r *foregroundObservedPageReader) Wait(cs []stateio.Completion, minCompletions int) ([]stateio.Completion, error) {
	r.observeWait()
	n := len(cs)
	cs, err := r.AsyncReader.Wait(cs, minCompletions)
	r.inflight -= len(cs) - n
	return cs, err
}

func (r *foregroundObservedPageReader) observeWait() {
	if !r.observed {
		r.observed = true
		r.firstWait <- r.inflight
	}
}

func (r *foregroundObservedPageReader) WaitInterruptible(cs []stateio.Completion, minCompletions int, wake <-chan struct{}) ([]stateio.Completion, bool, error) {
	r.observeWait()
	n := len(cs)
	cs, interrupted, err := r.AsyncReader.(stateio.InterruptibleAsyncReader).WaitInterruptible(cs, minCompletions, wake)
	r.inflight -= len(cs) - n
	return cs, interrupted, err
}

// TestAsyncPageLoadForegroundWithSpareCapacity checks that a slow main MF read
// does not prevent access to a subsequently registered private MF when the
// checkpoint reader still has capacity. This models private MF metadata becoming
// available after main MF page loading has started.
func TestAsyncPageLoadForegroundWithSpareCapacity(t *testing.T) {
	for name, opts := range map[string]AsyncPagesFileLoadOpts{
		"background":  {},
		"demand-only": {DisableBackground: true},
	} {
		t.Run(name, func(t *testing.T) { testForegroundWithSpareCapacity(t, opts) })
	}
}

func testForegroundWithSpareCapacity(t *testing.T, pageLoadOpts AsyncPagesFileLoadOpts) {
	t.Helper()
	pageSize := uint64(hostarch.PageSize)
	fr := memmap.FileRange{Start: 0, End: pageSize}
	want := bytes.Repeat([]byte{0x5a}, int(pageSize))

	// Save one MF image, then restore it twice at different pages file offsets.
	source := newForegroundWaitTestMemoryFile(t)
	allocated, err := source.Allocate(pageSize, AllocOpts{Mode: AllocateAndCommit})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.DecRef(allocated) })
	blocks, err := source.MapInternal(allocated, hostarch.Write)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := safemem.CopySeq(blocks, safemem.BlockSeqOf(safemem.BlockFromSafeSlice(want))); err != nil || n != pageSize {
		t.Fatalf("initializing source page: copied %d bytes, error: %v", n, err)
	}
	var metadata, pages bytes.Buffer
	saveDone := make(chan struct{})
	var saveErr error
	saver, err := StartAsyncPagesFileSave(stateio.NewIOWriter(&pages, pageSize, 1, 1), func(err error) {
		saveErr = err
		close(saveDone)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		saver.MemoryFilesDone()
		select {
		case <-saveDone:
		case <-time.After(foregroundWaitTestTimeout):
			t.Error("timed out waiting for page saver cleanup")
		}
	})
	if err := source.SaveTo(context.Background(), &metadata, &SaveOpts{PagesFile: saver}); err != nil {
		t.Fatal(err)
	}
	saver.MemoryFilesDone()
	select {
	case <-saveDone:
		if saveErr != nil {
			t.Fatal(saveErr)
		}
	case <-time.After(foregroundWaitTestTimeout):
		t.Fatal("timed out saving checkpoint page")
	}
	if pages.Len() != int(pageSize) {
		t.Fatalf("saved %d page bytes, want %d", pages.Len(), pageSize)
	}

	mainMF := newForegroundWaitTestMemoryFile(t)
	privateMF := newForegroundWaitTestMemoryFile(t)
	var mainLoaded, privateLoaded bool
	t.Cleanup(func() {
		if privateLoaded {
			privateMF.DecRef(fr)
		}
		if mainLoaded {
			mainMF.DecRef(fr)
		}
	})
	pageReader := &blockedForegroundPageReader{
		Reader:  bytes.NewReader(bytes.Repeat(pages.Bytes(), 2)),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseBackground := func() { releaseOnce.Do(func() { close(pageReader.release) }) }
	reader := &foregroundObservedPageReader{
		AsyncReader: stateio.NewIOReader(pageReader, pageSize, 1, 2),
		firstWait:   make(chan int, 1),
	}
	loadDone := make(chan struct{})
	var loadErr error
	loader, err := StartAsyncPagesFileLoad(reader, func(err error) {
		loadErr = err
		close(loadDone)
	}, nil, pageLoadOpts)
	if err != nil {
		t.Fatal(err)
	}
	accessDone := make(chan struct{})
	var accessStarted bool
	var mainAccessDone chan error
	t.Cleanup(func() {
		// Release the blocked I/O even when an assertion fails, before any MF
		// references or mappings are released.
		releaseBackground()
		loader.MemoryFilesDone()
		select {
		case <-loadDone:
			if loadErr != nil {
				t.Errorf("async page loading failed: %v", loadErr)
			}
		case <-time.After(foregroundWaitTestTimeout):
			t.Error("timed out waiting for page loader cleanup")
		}
		if accessStarted {
			select {
			case <-accessDone:
			case <-time.After(foregroundWaitTestTimeout):
				t.Error("timed out waiting for foreground access cleanup")
			}
		}
		if mainAccessDone != nil {
			select {
			case err := <-mainAccessDone:
				if err != nil {
					t.Errorf("main demand access failed: %v", err)
				}
			case <-time.After(foregroundWaitTestTimeout):
				t.Error("timed out waiting for main demand access cleanup")
			}
		}
	})
	opts := LoadOpts{PagesFile: loader}
	if err := mainMF.LoadFrom(context.Background(), bytes.NewReader(metadata.Bytes()), &opts); err != nil {
		t.Fatal(err)
	}
	mainLoaded = true
	if pageLoadOpts.DisableBackground {
		mainAccessDone = make(chan error, 1)
		go func() {
			_, err := mainMF.MapInternal(fr, hostarch.Read)
			mainAccessDone <- err
		}()
	}
	select {
	case inflight := <-reader.firstWait:
		if inflight != 1 || reader.MaxParallel() != 2 {
			t.Fatalf("initial in-flight reads = %d, capacity = %d; want 1 and 2", inflight, reader.MaxParallel())
		}
	case <-time.After(foregroundWaitTestTimeout):
		t.Fatal("loader did not wait for the main MF read")
	}
	select {
	case <-pageReader.started:
	case <-time.After(foregroundWaitTestTimeout):
		t.Fatal("main MF read did not start")
	}
	if err := privateMF.LoadFrom(context.Background(), bytes.NewReader(metadata.Bytes()), &opts); err != nil {
		t.Fatal(err)
	}
	privateLoaded = true
	loader.MemoryFilesDone()

	type accessResult struct {
		data []byte
		err  error
	}
	resultCh := make(chan accessResult, 1)
	accessStarted = true
	go func() {
		defer close(accessDone)
		blocks, err := privateMF.MapInternal(fr, hostarch.Read)
		if err != nil {
			resultCh <- accessResult{err: err}
			return
		}
		got := make([]byte, pageSize)
		_, err = safemem.CopySeq(safemem.BlockSeqOf(safemem.BlockFromSafeSlice(got)), blocks)
		resultCh <- accessResult{data: got, err: err}
	}()

	var result accessResult
	select {
	case result = <-resultCh:
	case <-time.After(foregroundWaitTestTimeout):
		loader.mu.Lock()
		waiters := loader.numWaiters
		priority := loader.priority.Len()
		loader.mu.Unlock()
		if waiters != 1 || priority != 1 {
			t.Fatalf("foreground access did not reach the expected priority wait: waiters=%d, priority ranges=%d", waiters, priority)
		}
		t.Errorf("foreground access stalled behind blocked background I/O despite a spare reader slot (waiters=%d, priority ranges=%d)", waiters, priority)
		// Confirm that releasing the unrelated I/O allows the same access to
		// complete successfully, rather than failing for an unrelated reason.
		releaseBackground()
		select {
		case result = <-resultCh:
		case <-time.After(foregroundWaitTestTimeout):
			t.Fatal("foreground access did not finish after releasing background I/O")
		}
	}
	if result.err != nil {
		t.Fatalf("foreground access failed: %v", result.err)
	}
	if !bytes.Equal(result.data, want) {
		t.Error("restored foreground page differs from checkpoint")
	}
}
