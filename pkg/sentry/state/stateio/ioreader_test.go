// Copyright 2025 The gVisor Authors.
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

package stateio

import (
	"bytes"
	"io"
	"math/bits"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"gvisor.dev/gvisor/pkg/rand"
	"gvisor.dev/gvisor/pkg/sentry/memmap"
)

func TestCompletionWaitInterruptPreservesReadyCompletions(t *testing.T) {
	completions := make(chan Completion, 2)
	completions <- Completion{ID: 1, N: 4096}
	wake := make(chan struct{}, 1)
	wake <- struct{}{} // Covers a wake arriving before the wait starts.
	cs, interrupted, err := CompletionChanWaitInterruptible(completions, []Completion{{ID: 9}}, 2, wake)
	if err != nil || !interrupted || len(cs) != 2 || cs[0].ID != 9 || cs[1].ID != 1 {
		t.Fatalf("interruptible wait = (%v, %t, %v), want prefix and ready completion", cs, interrupted, err)
	}
	completions <- Completion{ID: 2, N: 4096}
	cs, err = CompletionChanWait(completions, nil, 1)
	if err != nil || len(cs) != 1 || cs[0].ID != 2 {
		t.Fatalf("subsequent wait = (%v, %v), want only completion 2", cs, err)
	}
}

func TestCompletionWaitWithoutInterruptHonorsMinimum(t *testing.T) {
	completions := make(chan Completion)
	result := make(chan []Completion, 1)
	go func() {
		cs, _, _ := CompletionChanWaitInterruptible(completions, nil, 2, nil)
		result <- cs
	}()
	completions <- Completion{ID: 1}
	select {
	case cs := <-result:
		t.Fatalf("wait returned before two completions: %v", cs)
	default:
	}
	completions <- Completion{ID: 2}
	select {
	case cs := <-result:
		if len(cs) != 2 || cs[0].ID != 1 || cs[1].ID != 2 {
			t.Fatalf("wait completions = %v", cs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not return after its minimum was satisfied")
	}
}

func TestIOReaderRead(t *testing.T) {
	// Create random data.
	const chunkSize = 4096
	const dataLen = 1024 * chunkSize
	data := make([]byte, dataLen)
	_, _ = rand.Read(data)

	// Read data using async reads.
	r := NewIOReader(bytes.NewReader(data), chunkSize, 1 /* maxRanges */, 32 /* maxParallel */)
	defer r.Close()
	if r.NeedRegisterDestinationFD() {
		t.Fatalf("IOReader requires destination FD registration")
	}
	buf := make([]byte, dataLen)
	ids := uint32(0)
	off := 0
	done := 0
	var cs []Completion
	for done < dataLen {
		for ids != ^uint32(0) && off < dataLen {
			id := bits.TrailingZeros32(^ids)
			ids |= uint32(1) << id
			r.AddRead(id, int64(off), nil, memmap.FileRange{}, buf[off:off+chunkSize])
			off += chunkSize
		}
		cs, err := r.Wait(cs[:0], 1 /* minCompletions */)
		if err != nil {
			t.Fatalf("IOReader.Wait failed: %v", err)
		}
		for _, c := range cs {
			if c.Err != nil && c.Err != io.EOF {
				t.Fatalf("IOReader returned completion with error: %v", c.Err)
			}
			if c.N != chunkSize {
				t.Fatalf("IOReader returned completion of %d bytes, want %d", c.N, chunkSize)
			}
			ids &^= uint32(1) << c.ID
			done += chunkSize
		}
	}

	if !bytes.Equal(data, buf) {
		t.Errorf("Bytes differ")
	}
}

func TestIOReaderReadv(t *testing.T) {
	// Create random data.
	const chunkSize = 4096
	const dataLen = 1024 * chunkSize
	data := make([]byte, dataLen)
	_, _ = rand.Read(data)

	// Read data using async reads.
	r := NewIOReader(bytes.NewReader(data), chunkSize, 2 /* maxRanges */, 32 /* maxParallel */)
	defer r.Close()
	if r.NeedRegisterDestinationFD() {
		t.Fatalf("IOReader requires destination FD registration")
	}
	buf := make([]byte, dataLen)
	ids := uint32(0)
	off := 0
	done := 0
	var cs []Completion
	for done < dataLen {
		for ids != ^uint32(0) && off < dataLen {
			id := bits.TrailingZeros32(^ids)
			ids |= uint32(1) << id
			iovecs := []unix.Iovec{
				{Base: &buf[off], Len: chunkSize},
				{Base: &buf[off+chunkSize], Len: chunkSize},
			}
			r.AddReadv(id, int64(off), 2*chunkSize, nil, nil, iovecs)
			off += 2 * chunkSize
		}
		cs, err := r.Wait(cs[:0], 1 /* minCompletions */)
		if err != nil {
			t.Fatalf("IOReader.Wait failed: %v", err)
		}
		for _, c := range cs {
			if c.Err != nil && c.Err != io.EOF {
				t.Fatalf("IOReader returned completion with error: %v", c.Err)
			}
			if c.N != 2*chunkSize {
				t.Fatalf("IOReader returned completion of %d bytes, want %d", c.N, chunkSize)
			}
			ids &^= uint32(1) << c.ID
			done += 2 * chunkSize
		}
	}

	if !bytes.Equal(data, buf) {
		t.Errorf("Bytes differ")
	}
}
