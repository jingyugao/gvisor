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

//go:build !pagesize_64k

package pgalloc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/hostarch"
	"gvisor.dev/gvisor/pkg/memutil"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
	"gvisor.dev/gvisor/pkg/sentry/state/stateio"
)

func newSaveTestMemoryFile(t *testing.T, diskBacked bool) *MemoryFile {
	t.Helper()
	var file *os.File
	if diskBacked {
		var err error
		if file, err = os.CreateTemp(t.TempDir(), "pgalloc-save-test"); err != nil {
			t.Fatalf("CreateTemp: %v", err)
		}
	} else {
		fd, err := memutil.CreateMemFD("pgalloc-save-test", 0)
		if err != nil {
			t.Fatalf("CreateMemFD: %v", err)
		}
		file = os.NewFile(uintptr(fd), "pgalloc-save-test")
	}
	f, err := NewMemoryFile(file, MemoryFileOpts{
		DelayedEviction:         DelayedEvictionDisabled,
		DisableMemoryAccounting: true,
		DiskBackedFile:          diskBacked,
	})
	if err != nil {
		file.Close()
		t.Fatalf("NewMemoryFile: %v", err)
	}
	t.Cleanup(f.Destroy)
	return f
}

func allocateSaveTestPages(t *testing.T, f *MemoryFile) memmap.FileRange {
	t.Helper()
	fr, err := f.Allocate(16*uint64(hostarch.PageSize), AllocOpts{Mode: AllocateUncommitted})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	t.Cleanup(func() { f.DecRef(fr) })
	return fr
}

func pwritePage(t *testing.T, f *MemoryFile, fr memmap.FileRange, page int, b byte) {
	t.Helper()
	buf := make([]byte, hostarch.PageSize)
	buf[17] = b
	if _, err := unix.Pwrite(f.FD(), buf, int64(fr.Start)+int64(page*int(hostarch.PageSize))); err != nil {
		t.Fatalf("Pwrite page %d: %v", page, err)
	}
}

func TestHostFileDataSeeker(t *testing.T) {
	f := newSaveTestMemoryFile(t, false)
	fr := allocateSaveTestPages(t, f)
	pwritePage(t, f, fr, 2, 1)
	pwritePage(t, f, fr, 3, 1)
	pwritePage(t, f, fr, 9, 1)
	pwritePage(t, f, fr, 12, 0)
	page := func(n uint64) uint64 { return fr.Start + n*uint64(hostarch.PageSize) }
	d := f.newHostFileDataSeeker()
	for _, call := range []struct {
		off  uint64
		want memmap.FileRange
	}{
		{off: page(0), want: memmap.FileRange{page(2), page(4)}},
		{off: page(3), want: memmap.FileRange{page(3), page(4)}},
		{off: page(4), want: memmap.FileRange{page(9), page(10)}},
		{off: page(10), want: memmap.FileRange{page(12), page(13)}},
		{off: page(13), want: memmap.FileRange{f.TotalSize(), f.TotalSize()}},
		{off: page(15), want: memmap.FileRange{f.TotalSize(), f.TotalSize()}},
	} {
		got, err := d.dataAtOrAfter(call.off)
		if err != nil || got != call.want {
			t.Errorf("dataAtOrAfter(%#x): got (%v, %v), want (%v, nil)", call.off, got, err, call.want)
		}
	}
}

func TestSaveToPreservesContents(t *testing.T) {
	for _, tc := range []struct {
		name          string
		diskBacked    bool
		exclude       bool
		wantCommitted uint64
	}{
		{name: "memfd", wantCommitted: 2 * uint64(hostarch.PageSize)},
		{name: "memfd excluding committed zero pages", exclude: true, wantCommitted: uint64(hostarch.PageSize)},
		{name: "disk-backed", diskBacked: true, wantCommitted: 2 * uint64(hostarch.PageSize)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSaveTestMemoryFile(t, tc.diskBacked)
			fr := allocateSaveTestPages(t, f)
			pwritePage(t, f, fr, 2, 1)
			pwritePage(t, f, fr, 13, 2)
			if err := f.SaveTo(context.Background(), io.Discard, &SaveOpts{}); err != nil {
				t.Fatalf("first SaveTo: %v", err)
			}
			pwritePage(t, f, fr, 9, 0)
			pwritePage(t, f, fr, 13, 0)
			want := make([]byte, fr.Length())
			want[2*hostarch.PageSize+17] = 1

			var checkpoint bytes.Buffer
			if err := f.SaveTo(context.Background(), &checkpoint, &SaveOpts{ExcludeCommittedZeroPages: tc.exclude}); err != nil {
				t.Fatalf("SaveTo: %v", err)
			}
			if got := f.knownCommittedBytes; got != tc.wantCommitted {
				t.Errorf("knownCommittedBytes: got %d, want %d", got, tc.wantCommitted)
			}

			restored := newSaveTestMemoryFile(t, tc.diskBacked)
			if err := restored.LoadFrom(context.Background(), &checkpoint, &LoadOpts{}); err != nil {
				t.Fatalf("LoadFrom: %v", err)
			}
			t.Cleanup(func() { restored.DecRef(fr) })
			restored.forEachMappingSlice(fr, func(bs []byte) {
				if !bytes.Equal(bs, want) {
					t.Error("restored contents differ from saved contents")
				}
			})
		})
	}
}

func TestAsyncSaveToPreservesContents(t *testing.T) {
	for _, tc := range []struct {
		name          string
		diskBacked    bool
		exclude       bool
		wantCommitted uint64
	}{
		{name: "memfd", wantCommitted: 2 * uint64(hostarch.PageSize)},
		{name: "memfd excluding committed zero pages", exclude: true, wantCommitted: uint64(hostarch.PageSize)},
		{name: "disk-backed", diskBacked: true, wantCommitted: 2 * uint64(hostarch.PageSize)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSaveTestMemoryFile(t, tc.diskBacked)
			fr := allocateSaveTestPages(t, f)
			pwritePage(t, f, fr, 2, 1)
			pwritePage(t, f, fr, 13, 2)

			save := func(exclude bool) ([]byte, []byte) {
				var metadata bytes.Buffer
				var pages bytes.Buffer
				saveDone := make(chan error, 1)
				apfs, err := StartAsyncPagesFileSave(
					stateio.NewIOWriter(&pages, uint64(hostarch.PageSize), 1 /* maxRanges */, 2 /* maxParallel */),
					func(err error) { saveDone <- err },
				)
				if err != nil {
					t.Fatalf("StartAsyncPagesFileSave: %v", err)
				}
				saveErr := f.SaveTo(context.Background(), &metadata, &SaveOpts{
					ExcludeCommittedZeroPages: exclude,
					PagesFile:                 apfs,
				})
				apfs.MemoryFilesDone()
				asyncErr := <-saveDone
				if saveErr != nil {
					t.Fatalf("SaveTo: %v", saveErr)
				}
				if asyncErr != nil {
					t.Fatalf("asynchronous page save: %v", asyncErr)
				}
				if got, want := uint64(pages.Len()), f.knownCommittedBytes; got != want {
					t.Fatalf("pages file length is %d, want %d", got, want)
				}
				return metadata.Bytes(), pages.Bytes()
			}

			save(false)
			pwritePage(t, f, fr, 9, 0)
			pwritePage(t, f, fr, 13, 0)
			want := make([]byte, fr.Length())
			want[2*hostarch.PageSize+17] = 1
			metadata, pages := save(tc.exclude)
			if got := f.knownCommittedBytes; got != tc.wantCommitted {
				t.Errorf("knownCommittedBytes: got %d, want %d", got, tc.wantCommitted)
			}

			restored := newSaveTestMemoryFile(t, tc.diskBacked)
			loadDone := make(chan error, 1)
			apfl, err := StartAsyncPagesFileLoad(
				stateio.NewIOReader(bytes.NewReader(pages), uint64(hostarch.PageSize), 1 /* maxRanges */, 2 /* maxParallel */),
				func(err error) { loadDone <- err },
				nil,
				AsyncPagesFileLoadOpts{},
			)
			if err != nil {
				t.Fatalf("StartAsyncPagesFileLoad: %v", err)
			}
			loadErr := restored.LoadFrom(context.Background(), bytes.NewReader(metadata), &LoadOpts{PagesFile: apfl})
			apfl.MemoryFilesDone()
			asyncErr := <-loadDone
			if loadErr != nil {
				t.Fatalf("LoadFrom: %v", loadErr)
			}
			if asyncErr != nil {
				t.Fatalf("asynchronous page load: %v", asyncErr)
			}
			t.Cleanup(func() { restored.DecRef(fr) })
			restored.forEachMappingSlice(fr, func(bs []byte) {
				if !bytes.Equal(bs, want) {
					t.Error("restored contents differ from saved contents")
				}
			})
		})
	}
}

// gatedReaderAt holds reads until the test releases them and records maximum
// concurrent reads.
type gatedReaderAt struct {
	data      []byte
	fgOffset  int64
	started   chan int64
	bgRelease chan struct{}
	fgRelease chan struct{}
	mu        sync.Mutex
	active    int
	maxActive int
}

func (r *gatedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
	}()
	r.started <- off
	if off < r.fgOffset {
		<-r.bgRelease
	} else {
		<-r.fgRelease
	}
	return bytes.NewReader(r.data).ReadAt(p, off)
}

func (r *gatedReaderAt) maxInFlight() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxActive
}

// observedRead is a read submitted to an observedAsyncReader.
type observedRead struct {
	off  int64
	size uint64
	// inflightBefore is the number of bytes in submitted reads that had not
	// completed when this read was submitted.
	inflightBefore uint64
}

type observedAsyncReader struct {
	stateio.AsyncReader
	adds chan int64

	mu       sync.Mutex
	sizes    map[int]uint64
	inflight uint64
	reads    []observedRead
}

func (r *observedAsyncReader) observe(id int, off int64, size uint64) {
	r.mu.Lock()
	if r.sizes == nil {
		r.sizes = make(map[int]uint64)
	}
	r.reads = append(r.reads, observedRead{off: off, size: size, inflightBefore: r.inflight})
	r.sizes[id] = size
	r.inflight += size
	r.mu.Unlock()
	r.adds <- off
}

func (r *observedAsyncReader) snapshotReads() []observedRead {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]observedRead(nil), r.reads...)
}

func (r *observedAsyncReader) AddRead(id int, off int64, dstFile stateio.DestinationFile, dstFR memmap.FileRange, dstMap []byte) {
	r.observe(id, off, uint64(len(dstMap)))
	r.AsyncReader.AddRead(id, off, dstFile, dstFR, dstMap)
}

func (r *observedAsyncReader) AddReadv(id int, off int64, total uint64, dstFile stateio.DestinationFile, dstFRs []memmap.FileRange, dstMaps []unix.Iovec) {
	r.observe(id, off, total)
	r.AsyncReader.AddReadv(id, off, total, dstFile, dstFRs, dstMaps)
}

func (r *observedAsyncReader) Wait(cs []stateio.Completion, minCompletions int) ([]stateio.Completion, error) {
	n := len(cs)
	cs, err := r.AsyncReader.Wait(cs, minCompletions)
	r.mu.Lock()
	for _, c := range cs[n:] {
		r.inflight -= r.sizes[c.ID]
		delete(r.sizes, c.ID)
	}
	r.mu.Unlock()
	return cs, err
}

func waitForOffsets(t *testing.T, offsets <-chan int64, want int) []int64 {
	t.Helper()
	got := make([]int64, 0, want)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for len(got) < want {
		select {
		case off := <-offsets:
			got = append(got, off)
		case <-timer.C:
			t.Fatalf("timed out waiting for %d offsets; got %v", want, got)
		}
	}
	return got
}

func waitForLoadWaiter(t *testing.T, apfl *AsyncPagesFileLoad) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		apfl.mu.Lock()
		ready := apfl.numWaiters != 0
		apfl.mu.Unlock()
		if ready {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("timed out waiting for foreground load waiter")
		default:
			runtime.Gosched()
		}
	}
}

func TestAsyncPageLoadLimitsBackgroundSlots(t *testing.T) {
	const (
		backgroundPages = 16
		foregroundPages = 4
		slots           = 2
	)
	pageSize := uint64(hostarch.PageSize)
	for _, maxParallel := range []int{1, 2, 3, 12} {
		t.Run(fmt.Sprintf("MaxParallel%d", maxParallel), func(t *testing.T) {
			background := newSaveTestMemoryFile(t, false)
			backgroundFR := allocateSaveTestPages(t, background)
			for i := range backgroundPages {
				pwritePage(t, background, backgroundFR, i, byte(i+1))
			}
			foreground := newSaveTestMemoryFile(t, false)
			foregroundFR := allocateSaveTestPages(t, foreground)
			for i := range foregroundPages {
				pwritePage(t, foreground, foregroundFR, i, byte(i+101))
			}

			var pages bytes.Buffer
			saveDone := make(chan error, 1)
			apfs, err := StartAsyncPagesFileSave(stateio.NewIOWriter(&pages, pageSize, 1, 1), func(err error) { saveDone <- err })
			if err != nil {
				t.Fatalf("StartAsyncPagesFileSave: %v", err)
			}
			t.Cleanup(apfs.MemoryFilesDone)
			var backgroundMetadata, foregroundMetadata bytes.Buffer
			if err := background.SaveTo(context.Background(), &backgroundMetadata, &SaveOpts{PagesFile: apfs}); err != nil {
				t.Fatalf("background SaveTo: %v", err)
			}
			if err := foreground.SaveTo(context.Background(), &foregroundMetadata, &SaveOpts{PagesFile: apfs}); err != nil {
				t.Fatalf("foreground SaveTo: %v", err)
			}
			apfs.MemoryFilesDone()
			select {
			case err := <-saveDone:
				if err != nil {
					t.Fatalf("async page save: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for async page save")
			}

			bgRelease := make(chan struct{}, 1)
			fgRelease := make(chan struct{})
			var releaseOnce sync.Once
			releaseReads := func() {
				releaseOnce.Do(func() {
					close(bgRelease)
					close(fgRelease)
				})
			}
			defer releaseReads()
			readerAt := &gatedReaderAt{
				data:      pages.Bytes(),
				fgOffset:  int64(backgroundPages) * int64(pageSize),
				started:   make(chan int64, backgroundPages+foregroundPages),
				bgRelease: bgRelease,
				fgRelease: fgRelease,
			}
			ioReader := stateio.NewIOReader(readerAt, pageSize, 1, maxParallel)
			observed := &observedAsyncReader{AsyncReader: ioReader, adds: make(chan int64, backgroundPages+foregroundPages)}
			loadDone := make(chan struct{})
			var loadErr error
			apfl, err := StartAsyncPagesFileLoad(observed, func(err error) {
				loadErr = err
				close(loadDone)
			}, nil, AsyncPagesFileLoadOpts{BackgroundSlots: slots})
			if err != nil {
				t.Fatalf("StartAsyncPagesFileLoad: %v", err)
			}
			defer func() {
				apfl.MemoryFilesDone()
				releaseReads()
				select {
				case <-loadDone:
					if loadErr != nil {
						t.Errorf("async page load: %v", loadErr)
					}
				case <-time.After(10 * time.Second):
					t.Error("timed out waiting for async page load")
				}
			}()
			restoredBackground := newSaveTestMemoryFile(t, false)
			restoredForeground := newSaveTestMemoryFile(t, false)
			if err := restoredBackground.LoadFrom(context.Background(), &backgroundMetadata, &LoadOpts{PagesFile: apfl}); err != nil {
				t.Fatalf("background LoadFrom: %v", err)
			}
			if err := restoredForeground.LoadFrom(context.Background(), &foregroundMetadata, &LoadOpts{PagesFile: apfl, PagesFileOffset: uint64(backgroundPages) * pageSize}); err != nil {
				t.Fatalf("foreground LoadFrom: %v", err)
			}
			apfl.MemoryFilesDone()

			backgroundSlots := min(slots, maxParallel)
			bgOffsets := waitForOffsets(t, observed.adds, backgroundSlots)
			for _, off := range bgOffsets {
				if off < 0 || off >= int64(backgroundPages)*int64(pageSize) {
					t.Fatalf("initial background read has unexpected offset %d", off)
				}
			}
			waitForOffsets(t, readerAt.started, backgroundSlots)

			mapDone := make(chan error, 1)
			go func() {
				_, err := restoredForeground.MapInternal(memmap.FileRange{Start: foregroundFR.Start, End: foregroundFR.Start + uint64(foregroundPages)*pageSize}, hostarch.Read)
				mapDone <- err
			}()
			waitForLoadWaiter(t, apfl)
			// One completion wakes the loader. Awaited reads can then use all
			// available capacity, including slots that background reads may not
			// use.
			bgRelease <- struct{}{}
			foregroundSlots := min(foregroundPages, maxParallel-backgroundSlots+1)
			fgOffsets := waitForOffsets(t, observed.adds, foregroundSlots)
			for _, off := range fgOffsets {
				if off < int64(backgroundPages)*int64(pageSize) {
					t.Fatalf("priority read has background offset %d", off)
				}
			}
			waitForOffsets(t, readerAt.started, foregroundSlots)
			// Background reads may refill their own slots while awaited reads run.
			got := readerAt.maxInFlight()
			if lo, hi := max(backgroundSlots, backgroundSlots-1+foregroundSlots), min(maxParallel, backgroundSlots+foregroundSlots); got < lo || got > hi {
				t.Errorf("maximum concurrent reads: got %d, want in [%d, %d]", got, lo, hi)
			}
			releaseReads()
			select {
			case err := <-mapDone:
				if err != nil {
					t.Fatalf("MapInternal: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for foreground load")
			}
			if err := restoredForeground.AwaitLoadAll(); err != nil {
				t.Fatalf("AwaitLoadAll foreground: %v", err)
			}
			if err := restoredBackground.AwaitLoadAll(); err != nil {
				t.Fatalf("AwaitLoadAll background: %v", err)
			}
			for i, restored := range []*MemoryFile{restoredBackground, restoredForeground} {
				fr := backgroundFR
				pages := backgroundPages
				value := byte(1)
				if i == 1 {
					fr = foregroundFR
					pages = foregroundPages
					value = 101
				}
				t.Cleanup(func() { restored.DecRef(fr) })
				want := make([]byte, int(fr.Length()))
				for page := range pages {
					want[page*hostarch.PageSize+17] = value + byte(page)
				}
				restored.forEachMappingSlice(fr, func(got []byte) {
					if !bytes.Equal(got, want) {
						t.Errorf("restored MemoryFile %d contents differ", i)
					}
				})
			}
		})
	}
}

// TestAsyncPageLoadBackgroundDelaysForeground checks how much background read
// data is queued ahead of an awaited read.
//
// Reads are served in order by the storage device, so an awaited read waits for
// all earlier reads, and its extra latency is the bytes queued ahead of it
// divided by the device bandwidth. Without a background limit, a full set of
// background reads is queued ahead. With the limit, only a few small ones are.
func TestAsyncPageLoadBackgroundDelaysForeground(t *testing.T) {
	const (
		backgroundBytes = 64 << 20
		foregroundPages = 4
		maxReadBytes    = 1 << 20
		maxParallel     = 12
		bandwidth       = 200e6
	)
	pageSize := uint64(hostarch.PageSize)

	for _, tc := range []struct {
		name  string
		slots int
	}{
		{"Unlimited", 0},
		{"Limited", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			background := newSaveTestMemoryFile(t, false)
			bgFR, err := background.Allocate(backgroundBytes, AllocOpts{Mode: AllocateUncommitted})
			if err != nil {
				t.Fatalf("Allocate: %v", err)
			}
			t.Cleanup(func() { background.DecRef(bgFR) })
			buf := bytes.Repeat([]byte{1}, 1<<20)
			for off := uint64(0); off < backgroundBytes; off += uint64(len(buf)) {
				if _, err := unix.Pwrite(background.FD(), buf, int64(bgFR.Start+off)); err != nil {
					t.Fatalf("Pwrite: %v", err)
				}
			}
			foreground := newSaveTestMemoryFile(t, false)
			fgFR := allocateSaveTestPages(t, foreground)
			for i := range foregroundPages {
				pwritePage(t, foreground, fgFR, i, byte(i+101))
			}

			var pages bytes.Buffer
			saveDone := make(chan error, 1)
			apfs, err := StartAsyncPagesFileSave(stateio.NewIOWriter(&pages, maxReadBytes, 16, 1), func(err error) { saveDone <- err })
			if err != nil {
				t.Fatalf("StartAsyncPagesFileSave: %v", err)
			}
			var bgMeta, fgMeta bytes.Buffer
			if err := background.SaveTo(context.Background(), &bgMeta, &SaveOpts{PagesFile: apfs}); err != nil {
				t.Fatalf("background SaveTo: %v", err)
			}
			if err := foreground.SaveTo(context.Background(), &fgMeta, &SaveOpts{PagesFile: apfs}); err != nil {
				t.Fatalf("foreground SaveTo: %v", err)
			}
			apfs.MemoryFilesDone()
			if err := <-saveDone; err != nil {
				t.Fatalf("async page save: %v", err)
			}

			// Background reads block until released one at a time; awaited reads
			// complete immediately.
			bgRelease := make(chan struct{}, maxParallel)
			fgRelease := make(chan struct{})
			close(fgRelease)
			var releaseOnce sync.Once
			releaseAll := func() { releaseOnce.Do(func() { close(bgRelease) }) }
			defer releaseAll()
			readerAt := &gatedReaderAt{
				data:      pages.Bytes(),
				fgOffset:  backgroundBytes,
				started:   make(chan int64, 4096),
				bgRelease: bgRelease,
				fgRelease: fgRelease,
			}
			observed := &observedAsyncReader{
				AsyncReader: stateio.NewIOReader(readerAt, maxReadBytes, 16, maxParallel),
				adds:        make(chan int64, 4096),
			}
			loadDone := make(chan error, 1)
			apfl, err := StartAsyncPagesFileLoad(observed, func(err error) { loadDone <- err }, nil, AsyncPagesFileLoadOpts{BackgroundSlots: tc.slots})
			if err != nil {
				t.Fatalf("StartAsyncPagesFileLoad: %v", err)
			}
			restoredBackground := newSaveTestMemoryFile(t, false)
			restoredForeground := newSaveTestMemoryFile(t, false)
			if err := restoredBackground.LoadFrom(context.Background(), bytes.NewReader(bgMeta.Bytes()), &LoadOpts{PagesFile: apfl}); err != nil {
				t.Fatalf("background LoadFrom: %v", err)
			}
			if err := restoredForeground.LoadFrom(context.Background(), bytes.NewReader(fgMeta.Bytes()), &LoadOpts{PagesFile: apfl, PagesFileOffset: backgroundBytes}); err != nil {
				t.Fatalf("foreground LoadFrom: %v", err)
			}
			apfl.MemoryFilesDone()
			t.Cleanup(func() {
				restoredBackground.DecRef(bgFR)
				restoredForeground.DecRef(fgFR)
			})

			// Background reads fill every slot they may use, then stop.
			wantBackground := maxParallel
			if tc.slots > 0 {
				wantBackground = tc.slots
			}
			waitForOffsets(t, observed.adds, wantBackground)
			time.Sleep(100 * time.Millisecond)
			if got := len(observed.snapshotReads()); got != wantBackground {
				t.Fatalf("background reads in flight: got %d, want %d", got, wantBackground)
			}

			// A foreground fault arrives; one background completion wakes the
			// loader, which then submits the awaited read.
			mapDone := make(chan error, 1)
			go func() {
				_, err := restoredForeground.MapInternal(memmap.FileRange{Start: fgFR.Start, End: fgFR.Start + pageSize}, hostarch.Read)
				mapDone <- err
			}()
			waitForLoadWaiter(t, apfl)
			bgRelease <- struct{}{}
			var fgRead observedRead
			deadline := time.Now().Add(10 * time.Second)
			for found := false; !found; {
				for _, r := range observed.snapshotReads() {
					if r.off >= backgroundBytes {
						fgRead, found = r, true
						break
					}
				}
				if !found {
					if time.Now().After(deadline) {
						t.Fatal("timed out waiting for the awaited read to be submitted")
					}
					time.Sleep(time.Millisecond)
				}
			}
			ahead := fgRead.inflightBefore
			t.Logf("%d KiB of background reads queued ahead of the awaited read: %v extra latency at %.0f MB/s",
				ahead>>10, time.Duration(float64(ahead)/bandwidth*float64(time.Second)).Round(time.Millisecond), bandwidth/1e6)
			if tc.slots == 0 {
				// Roughly a full set of maximum-size reads.
				if want := uint64(maxParallel-2) * maxReadBytes; ahead < want {
					t.Errorf("unlimited background reads queued only %d bytes ahead of the awaited read, want at least %d", ahead, want)
				}
			} else if limit := uint64(tc.slots-1) * maxReadBytes; ahead > limit {
				t.Errorf("limited background reads queued %d bytes ahead of the awaited read, want at most %d", ahead, limit)
			}

			releaseAll()
			select {
			case err := <-mapDone:
				if err != nil {
					t.Fatalf("MapInternal: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("timed out waiting for the awaited load")
			}
			if err := <-loadDone; err != nil {
				t.Fatalf("async page load: %v", err)
			}
		})
	}
}

// latencyReaderAt simulates a storage device with a fixed per-read latency and
// a total bandwidth shared by all reads, which are served in FIFO order.
type latencyReaderAt struct {
	data      []byte
	latency   time.Duration
	bandwidth float64 // bytes per second, shared by all reads

	mu   sync.Mutex
	busy time.Time // when the device finishes the reads accepted so far
}

// reserve queues a read of n bytes behind all earlier reads and returns when it
// completes.
func (r *latencyReaderAt) reserve(n int) time.Time {
	xfer := time.Duration(float64(n) / r.bandwidth * float64(time.Second))
	r.mu.Lock()
	defer r.mu.Unlock()
	start := time.Now()
	if r.busy.After(start) {
		start = r.busy
	}
	r.busy = start.Add(xfer)
	return r.busy.Add(r.latency)
}

func (r *latencyReaderAt) ReadAt(p []byte, off int64) (int, error) {
	time.Sleep(time.Until(r.reserve(len(p))))
	return bytes.NewReader(r.data).ReadAt(p, off)
}

// runNeighbor queues 1 MiB reads on r at a fixed rate of share*r.bandwidth,
// regardless of how long they take, until stop is closed.
func (r *latencyReaderAt) runNeighbor(share float64, stop <-chan struct{}) {
	const readBytes = 1 << 20
	interval := time.Duration(float64(readBytes) / (share * r.bandwidth) * float64(time.Second))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			r.reserve(readBytes)
		}
	}
}

// TestAsyncPageLoadRandomForeground measures the latency of random single-page
// faults in the part of a MemoryFile that background loading has not reached
// yet, while the rest of the file loads in the background from a disk shared
// with a neighbor. It only runs with PGALLOC_LOAD_LATENCY_TEST=1.
func TestAsyncPageLoadRandomForeground(t *testing.T) {
	if os.Getenv("PGALLOC_LOAD_LATENCY_TEST") != "1" {
		t.Skip("set PGALLOC_LOAD_LATENCY_TEST=1 to run")
	}
	const (
		totalBytes  = 512 << 20
		maxParallel = 12
		bandwidth   = 200e6
	)
	pageSize := uint64(hostarch.PageSize)

	f := newSaveTestMemoryFile(t, false)
	fr, err := f.Allocate(totalBytes, AllocOpts{Mode: AllocateUncommitted})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	t.Cleanup(func() { f.DecRef(fr) })
	buf := bytes.Repeat([]byte{1}, 1<<20)
	for off := uint64(0); off < totalBytes; off += uint64(len(buf)) {
		if _, err := unix.Pwrite(f.FD(), buf, int64(fr.Start+off)); err != nil {
			t.Fatalf("Pwrite: %v", err)
		}
	}
	var pages bytes.Buffer
	pages.Grow(totalBytes)
	saveDone := make(chan error, 1)
	apfs, err := StartAsyncPagesFileSave(stateio.NewIOWriter(&pages, 1<<20, 16, 1), func(err error) { saveDone <- err })
	if err != nil {
		t.Fatalf("StartAsyncPagesFileSave: %v", err)
	}
	var meta bytes.Buffer
	if err := f.SaveTo(context.Background(), &meta, &SaveOpts{PagesFile: apfs}); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}
	apfs.MemoryFilesDone()
	if err := <-saveDone; err != nil {
		t.Fatalf("async page save: %v", err)
	}

	// Faulting pages are random pages in the upper half, which background
	// loading (which proceeds in file order) reaches last.
	half := uint64(totalBytes) / 2 / pageSize
	order := rand.New(rand.NewSource(1)).Perm(int(half))

	const readBytes = 4 << 20
	run := func(t *testing.T, slots, workers int, neighborShare float64) {
		dev := &latencyReaderAt{data: pages.Bytes(), latency: 2 * time.Millisecond, bandwidth: bandwidth}
		stopNeighbor := make(chan struct{})
		defer close(stopNeighbor)
		if neighborShare > 0 {
			go dev.runNeighbor(neighborShare, stopNeighbor)
		}

		done := make(chan error, 1)
		apfl, err := StartAsyncPagesFileLoad(stateio.NewIOReader(dev, readBytes, 16, maxParallel), func(err error) { done <- err }, nil, AsyncPagesFileLoadOpts{BackgroundSlots: slots})
		if err != nil {
			t.Fatalf("StartAsyncPagesFileLoad: %v", err)
		}
		rf := newSaveTestMemoryFile(t, false)
		if err := rf.LoadFrom(context.Background(), bytes.NewReader(meta.Bytes()), &LoadOpts{PagesFile: apfl}); err != nil {
			t.Fatalf("LoadFrom: %v", err)
		}
		apfl.MemoryFilesDone()
		t.Cleanup(func() { rf.DecRef(fr) })
		time.Sleep(200 * time.Millisecond)

		var mu sync.Mutex
		next := 0
		var lats []time.Duration
		var wg sync.WaitGroup
		start := time.Now()
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					apfl.mu.Lock()
					loaded := apfl.bytesLoaded
					apfl.mu.Unlock()
					mu.Lock()
					i := next
					next++
					mu.Unlock()
					if loaded >= totalBytes/2 || i >= len(order) {
						return
					}
					pg := memmap.FileRange{Start: fr.Start + totalBytes/2 + uint64(order[i])*pageSize}
					pg.End = pg.Start + pageSize
					t0 := time.Now()
					if _, err := rf.MapInternal(pg, hostarch.Read); err != nil {
						t.Errorf("MapInternal: %v", err)
						return
					}
					lat := time.Since(t0)
					mu.Lock()
					lats = append(lats, lat)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		window := time.Since(start)
		if len(lats) == 0 {
			t.Fatal("no faults measured")
		}
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		rate := float64(len(lats)) / window.Seconds()
		if err := <-done; err != nil {
			t.Fatalf("async page load: %v", err)
		}
		t.Logf("%d faults (%.1f faults/s); latency p50 %v p99 %v; whole load %v",
			len(lats), rate,
			lats[len(lats)/2].Round(time.Millisecond), lats[len(lats)*99/100].Round(time.Millisecond),
			time.Since(start).Round(100*time.Millisecond))
	}
	for _, slots := range []int{0 /* unlimited */, 1, 2, 4, 8} {
		for _, sc := range []struct {
			name     string
			workers  int
			neighbor float64
		}{
			{"shared/workers1", 1, 0.7},
			{"shared/workers8", 8, 0.7},
			{"idle/workers1", 1, 0},
		} {
			t.Run(fmt.Sprintf("slots%d/%s", slots, sc.name), func(t *testing.T) {
				run(t, slots, sc.workers, sc.neighbor)
			})
		}
	}
}
