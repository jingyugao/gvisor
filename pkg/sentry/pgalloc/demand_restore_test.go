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
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/context"
	"gvisor.dev/gvisor/pkg/errors/linuxerr"
	"gvisor.dev/gvisor/pkg/hostarch"
	"gvisor.dev/gvisor/pkg/memutil"
	"gvisor.dev/gvisor/pkg/safemem"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
	"gvisor.dev/gvisor/pkg/sentry/state/stateio"
)

const asyncPageLoadTestTimeout = 5 * time.Second

type asyncPageCheckpoint struct {
	metadata []byte
	pages    []byte
	contents [][]byte
	fr       memmap.FileRange
}

func newAsyncPageLoadTestMemoryFile(t *testing.T) *MemoryFile {
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
	return mf
}

func makeAsyncPageCheckpoint(t *testing.T, numPages int) asyncPageCheckpoint {
	t.Helper()
	pageSize := uint64(hostarch.PageSize)
	source := newAsyncPageLoadTestMemoryFile(t)
	defer source.Destroy()
	fr, err := source.Allocate(uint64(numPages)*pageSize, AllocOpts{Mode: AllocateAndCommit})
	if err != nil {
		t.Fatal(err)
	}
	defer source.DecRef(fr)
	contents := make([][]byte, numPages)
	blocks, err := source.MapInternal(fr, hostarch.Write)
	if err != nil {
		t.Fatal(err)
	}
	for i := range contents {
		contents[i] = bytes.Repeat([]byte{byte(i + 0x41)}, int(pageSize))
	}
	var checkpointData []byte
	for _, page := range contents {
		checkpointData = append(checkpointData, page...)
	}
	if n, err := safemem.CopySeq(blocks, safemem.BlockSeqOf(safemem.BlockFromSafeSlice(checkpointData))); err != nil || uint64(n) != fr.Length() {
		t.Fatalf("initializing checkpoint pages: copied %d bytes, error: %v", n, err)
	}

	var metadata, pages bytes.Buffer
	done := make(chan error, 1)
	saver, err := StartAsyncPagesFileSave(stateio.NewIOWriter(&pages, pageSize, 1, 1), func(err error) { done <- err })
	if err != nil {
		t.Fatal(err)
	}
	saverDoneRead := false
	defer func() {
		if !saverDoneRead {
			saver.MemoryFilesDone()
			waitAsyncPageLoadTest(t, done, "page saver cleanup")
		}
	}()
	if err := source.SaveTo(context.Background(), &metadata, &SaveOpts{PagesFile: saver}); err != nil {
		t.Fatal(err)
	}
	saver.MemoryFilesDone()
	if err := waitAsyncPageLoadTest(t, done, "saving checkpoint pages"); err != nil {
		t.Fatal(err)
	}
	saverDoneRead = true
	return asyncPageCheckpoint{metadata: metadata.Bytes(), pages: pages.Bytes(), contents: contents, fr: fr}
}

type observedPageReader struct {
	mu          sync.Mutex
	reader      *bytes.Reader
	offsets     []int64
	started     chan struct{}
	release     chan struct{}
	closed      chan struct{}
	once        sync.Once
	releaseOnce sync.Once
	err         error
}

func newObservedPageReader(data []byte) *observedPageReader {
	return &observedPageReader{
		reader:  bytes.NewReader(data),
		started: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (r *observedPageReader) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.offsets = append(r.offsets, off)
	r.mu.Unlock()
	r.once.Do(func() { close(r.started) })
	if r.err != nil {
		<-r.release
		return 0, r.err
	}
	return r.reader.ReadAt(p, off)
}

func (r *observedPageReader) Close() error {
	close(r.closed)
	return nil
}

func (r *observedPageReader) unblock() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func (r *observedPageReader) readOffsets() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.offsets...)
}

// observedAsyncReader records enqueue requests synchronously. ReaderAt alone
// cannot prove that untouched pages stayed cold: an enqueued read may not have
// reached an IOReader worker before the demand read completes.
type observedAsyncReader struct {
	stateio.AsyncReader
	mu          sync.Mutex
	submissions []int64
}

func (r *observedAsyncReader) record(off int64) {
	r.mu.Lock()
	r.submissions = append(r.submissions, off)
	r.mu.Unlock()
}

func (r *observedAsyncReader) AddRead(id int, off int64, df stateio.DestinationFile, fr memmap.FileRange, dst []byte) {
	r.record(off)
	r.AsyncReader.AddRead(id, off, df, fr, dst)
}

func (r *observedAsyncReader) AddReadv(id int, off int64, total uint64, df stateio.DestinationFile, frs []memmap.FileRange, dst []unix.Iovec) {
	r.record(off)
	r.AsyncReader.AddReadv(id, off, total, df, frs, dst)
}

func (r *observedAsyncReader) readSubmissions() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.submissions...)
}

type lazyPageRestore struct {
	mf          *MemoryFile
	loader      *AsyncPagesFileLoad
	asyncReader *observedAsyncReader
	reader      *observedPageReader
	fr          memmap.FileRange
	pageSize    uint64
	done        chan error
	fileDone    chan error
	doneRead    bool
	released    map[uint64]bool
}

func newLazyPageRestore(t *testing.T, checkpoint asyncPageCheckpoint, reader *observedPageReader) *lazyPageRestore {
	return newPageRestore(t, checkpoint, reader, AsyncPagesFileLoadOpts{DisableBackground: true})
}

func newPageRestore(t *testing.T, checkpoint asyncPageCheckpoint, reader *observedPageReader, opts AsyncPagesFileLoadOpts) *lazyPageRestore {
	t.Helper()
	pageSize := uint64(hostarch.PageSize)
	mf := newAsyncPageLoadTestMemoryFile(t)
	done := make(chan error, 1)
	fileDone := make(chan error, 1)
	asyncReader := &observedAsyncReader{AsyncReader: stateio.NewIOReader(reader, pageSize, 1, 4)}
	loader, err := StartAsyncPagesFileLoad(
		asyncReader,
		func(err error) { done <- err },
		nil,
		opts,
	)
	if err != nil {
		t.Fatal(err)
	}
	restored := &lazyPageRestore{
		mf:          mf,
		loader:      loader,
		asyncReader: asyncReader,
		reader:      reader,
		fr:          checkpoint.fr,
		pageSize:    pageSize,
		done:        done,
		fileDone:    fileDone,
		released:    make(map[uint64]bool),
	}
	if err := mf.LoadFrom(context.Background(), bytes.NewReader(checkpoint.metadata), &LoadOpts{
		PagesFile:    loader,
		DoneCallback: func(err error) { fileDone <- err },
	}); err != nil {
		reader.unblock()
		loader.MemoryFilesDone()
		waitAsyncPageLoadTest(t, done, "loader after failed metadata load")
		mf.Destroy()
		t.Fatal(err)
	}
	return restored
}

func (r *lazyPageRestore) release(fr memmap.FileRange) {
	for off := fr.Start; off < fr.End; off += r.pageSize {
		if r.released[off] {
			panic(fmt.Sprintf("test released page %#x twice", off))
		}
		r.mf.DecRef(memmap.FileRange{Start: off, End: off + r.pageSize})
		r.released[off] = true
	}
}

func (r *lazyPageRestore) waitDone(t *testing.T, what string) error {
	err := waitAsyncPageLoadTest(t, r.done, what)
	r.doneRead = true
	return err
}

func (r *lazyPageRestore) cleanup() {
	// A failed read can leave reader.ReadAt blocked. Release it before dropping
	// references, since demand waiters hold those references until they wake.
	r.reader.unblock()
	for off := r.fr.Start; off < r.fr.End; off += r.pageSize {
		if !r.released[off] {
			r.mf.DecRef(memmap.FileRange{Start: off, End: off + r.pageSize})
			r.released[off] = true
		}
	}
	r.loader.MemoryFilesDone()
	if !r.doneRead {
		waitAsyncPageLoadTest(nil, r.done, "page loader cleanup")
	}
	select {
	case <-r.reader.closed:
	case <-time.After(asyncPageLoadTestTimeout):
		panic("timed out waiting for pages reader close")
	}
	r.mf.Destroy()
}

func waitAsyncPageLoadTest(t *testing.T, done <-chan error, what string) error {
	if t != nil {
		t.Helper()
	}
	select {
	case err := <-done:
		return err
	case <-time.After(asyncPageLoadTestTimeout):
		if t != nil {
			t.Fatalf("timed out waiting for %s", what)
		}
		panic("timed out waiting for " + what)
		return nil
	}
}

func readMemoryFilePage(t *testing.T, mf *MemoryFile, fr memmap.FileRange) []byte {
	t.Helper()
	blocks, err := mf.MapInternal(fr, hostarch.Read)
	if err != nil {
		t.Fatalf("MapInternal(%v): %v", fr, err)
	}
	got := make([]byte, fr.Length())
	if n, err := safemem.CopySeq(safemem.BlockSeqOf(safemem.BlockFromSafeSlice(got)), blocks); err != nil || uint64(n) != fr.Length() {
		t.Fatalf("reading MemoryFile page: copied %d bytes, error: %v", n, err)
	}
	return got
}

func TestAsyncPageLoadDemandOnly(t *testing.T) {
	checkpoint := makeAsyncPageCheckpoint(t, 3)
	reader := newObservedPageReader(checkpoint.pages)
	restored := newLazyPageRestore(t, checkpoint, reader)
	defer restored.cleanup()
	restored.loader.MemoryFilesDone()
	assertNoAsyncPageReadSubmission(t, restored.asyncReader, "before demand")
	if offsets := reader.readOffsets(); len(offsets) != 0 {
		t.Fatalf("page reads before demand: %v, want none", offsets)
	}

	fr := memmap.FileRange{Start: checkpoint.fr.Start + restored.pageSize, End: checkpoint.fr.Start + 2*restored.pageSize}
	if got, want := readMemoryFilePage(t, restored.mf, fr), checkpoint.contents[1]; !bytes.Equal(got, want) {
		t.Fatalf("demand-loaded page contents differ from checkpoint")
	}
	wantOffset := int64(restored.pageSize)
	if offsets := restored.asyncReader.readSubmissions(); len(offsets) != 1 || offsets[0] != wantOffset {
		t.Fatalf("page read submissions = %v, want [%d]", offsets, wantOffset)
	}
	if offsets := reader.readOffsets(); len(offsets) != 1 || offsets[0] != wantOffset {
		t.Fatalf("page read offsets = %v, want [%d]", offsets, wantOffset)
	}
	// Cold neighbors remain available for discard and were not included in the
	// demanded read.
	restored.release(memmap.FileRange{Start: checkpoint.fr.Start, End: fr.Start})
	restored.release(memmap.FileRange{Start: fr.End, End: checkpoint.fr.End})
	if err := restored.waitDone(t, "completion after discarding cold pages"); err != nil {
		t.Fatal(err)
	}
	if offsets := restored.asyncReader.readSubmissions(); len(offsets) != 1 {
		t.Fatalf("discarding cold pages submitted extra reads: %v", offsets)
	}
}

func TestAsyncPageLoadDiscardColdPages(t *testing.T) {
	checkpoint := makeAsyncPageCheckpoint(t, 3)
	reader := newObservedPageReader(checkpoint.pages)
	restored := newLazyPageRestore(t, checkpoint, reader)
	defer restored.cleanup()
	restored.loader.MemoryFilesDone()
	assertNoAsyncPageReadSubmission(t, restored.asyncReader, "before discarding cold pages")
	restored.release(memmap.FileRange{Start: checkpoint.fr.Start, End: checkpoint.fr.End - restored.pageSize})
	select {
	case err := <-restored.done:
		restored.doneRead = true
		t.Fatalf("loader completed before final cold page was discarded: %v", err)
	default:
	}
	restored.release(memmap.FileRange{Start: checkpoint.fr.End - restored.pageSize, End: checkpoint.fr.End})
	if err := restored.waitDone(t, "completion after discarding all cold pages"); err != nil {
		t.Fatal(err)
	}
	if err := waitAsyncPageLoadTest(t, restored.fileDone, "MemoryFile completion after discard"); err != nil {
		t.Fatal(err)
	}
	if restored.mf.IsAsyncLoading() {
		t.Fatal("discarding all cold pages left the MemoryFile attached to the loader")
	}
	if offsets := restored.asyncReader.readSubmissions(); len(offsets) != 0 {
		t.Fatalf("discarding untouched pages submitted reads: %v", offsets)
	}
	select {
	case <-reader.closed:
	default:
		t.Fatal("pages reader was not closed after all pages were discarded")
	}
}

func TestAsyncPageLoadCheckpointLoadsColdPages(t *testing.T) {
	checkpoint := makeAsyncPageCheckpoint(t, 3)
	reader := newObservedPageReader(checkpoint.pages)
	restored := newLazyPageRestore(t, checkpoint, reader)
	defer restored.cleanup()
	restored.loader.MemoryFilesDone()
	// Saving a still-lazy MemoryFile must materialize its cold pages without
	// requiring callers to explicitly drain the old checkpoint first.
	var recheckpoint bytes.Buffer
	if err := restored.mf.SaveTo(context.Background(), &recheckpoint, &SaveOpts{}); err != nil {
		t.Fatalf("SaveTo while pages are cold: %v", err)
	}
	if offsets := restored.asyncReader.readSubmissions(); len(offsets) != len(checkpoint.contents) {
		t.Fatalf("AwaitLoadAll submitted reads at %v, want %d page reads", offsets, len(checkpoint.contents))
	}
	for i, want := range checkpoint.contents {
		fr := memmap.FileRange{Start: checkpoint.fr.Start + uint64(i)*restored.pageSize, End: checkpoint.fr.Start + uint64(i+1)*restored.pageSize}
		if got := readMemoryFilePage(t, restored.mf, fr); !bytes.Equal(got, want) {
			t.Errorf("page %d differs from checkpoint", i)
		}
	}

	second := newAsyncPageLoadTestMemoryFile(t)
	defer func() {
		second.DecRef(checkpoint.fr)
		second.Destroy()
	}()
	if err := second.LoadFrom(context.Background(), bytes.NewReader(recheckpoint.Bytes()), &LoadOpts{}); err != nil {
		t.Fatalf("LoadFrom re-checkpoint: %v", err)
	}
	for i, want := range checkpoint.contents {
		fr := memmap.FileRange{Start: checkpoint.fr.Start + uint64(i)*restored.pageSize, End: checkpoint.fr.Start + uint64(i+1)*restored.pageSize}
		if got := readMemoryFilePage(t, second, fr); !bytes.Equal(got, want) {
			t.Errorf("re-checkpointed page %d differs from source", i)
		}
	}
	if err := restored.waitDone(t, "loader completion after AwaitLoadAll"); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncPageLoadErrorWakesWaiters(t *testing.T) {
	checkpoint := makeAsyncPageCheckpoint(t, 2)
	reader := newObservedPageReader(checkpoint.pages)
	reader.err = errors.New("injected checkpoint read failure")
	restored := newLazyPageRestore(t, checkpoint, reader)
	var accessWG sync.WaitGroup
	defer func() {
		reader.unblock()
		accessWG.Wait()
		restored.cleanup()
	}()
	restored.loader.MemoryFilesDone()
	fr := memmap.FileRange{Start: checkpoint.fr.Start, End: checkpoint.fr.Start + restored.pageSize}
	results := make(chan error, 2)
	for range 2 {
		accessWG.Add(1)
		go func() {
			defer accessWG.Done()
			_, err := restored.mf.MapInternal(fr, hostarch.Read)
			results <- err
		}()
	}
	select {
	case <-reader.started:
	case <-time.After(asyncPageLoadTestTimeout):
		t.Fatal("demand read did not start")
	}
	waitForAsyncPageLoadWaiters(t, restored.loader, 2)
	reader.unblock()
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, linuxerr.EIO) {
				t.Fatalf("MapInternal error = %v, want EIO after deferred read failure", err)
			}
		case <-time.After(asyncPageLoadTestTimeout):
			t.Fatal("page read failure did not wake all waiters")
		}
	}
	if err := restored.waitDone(t, "loader failure callback"); err == nil {
		t.Fatal("loader callback returned nil after a page read failure")
	}
}

func waitForAsyncPageLoadWaiters(t *testing.T, loader *AsyncPagesFileLoad, want int) {
	t.Helper()
	deadline := time.Now().Add(asyncPageLoadTestTimeout)
	for {
		loader.mu.Lock()
		got := loader.numWaiters
		loader.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("async page load has %d waiters, want %d", got, want)
		}
		runtime.Gosched()
	}
}

func TestAsyncPageLoadZeroOptsRemainsEager(t *testing.T) {
	checkpoint := makeAsyncPageCheckpoint(t, 3)
	reader := newObservedPageReader(checkpoint.pages)
	restored := newPageRestore(t, checkpoint, reader, AsyncPagesFileLoadOpts{})
	defer restored.cleanup()
	restored.loader.MemoryFilesDone()
	if err := restored.waitDone(t, "eager loading with zero options"); err != nil {
		t.Fatal(err)
	}
	if offsets := restored.asyncReader.readSubmissions(); len(offsets) != len(checkpoint.contents) {
		t.Fatalf("zero options submitted offsets %v, want %d page reads", offsets, len(checkpoint.contents))
	}
	for i, want := range checkpoint.contents {
		fr := memmap.FileRange{Start: checkpoint.fr.Start + uint64(i)*restored.pageSize, End: checkpoint.fr.Start + uint64(i+1)*restored.pageSize}
		if got := readMemoryFilePage(t, restored.mf, fr); !bytes.Equal(got, want) {
			t.Errorf("eagerly loaded page %d differs from checkpoint", i)
		}
	}
}

func assertNoAsyncPageReadSubmission(t *testing.T, reader *observedAsyncReader, when string) {
	t.Helper()
	// There is no production idle hook; give the async loader an explicit
	// scheduling window after MemoryFilesDone and fail if it submits speculative
	// work before any page is demanded.
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	if offsets := reader.readSubmissions(); len(offsets) != 0 {
		t.Fatalf("page reads submitted %s: %v", when, offsets)
	}
}
