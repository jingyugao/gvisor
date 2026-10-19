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

package checkpointprefetch

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const mapChunkPages = 16 * 1024

func residentRanges(ctx context.Context, f *checkpointFile, pageSize uint64) ([]Range, error) {
	var ranges []Range
	var active *Range
	chunkMax := uint64(mapChunkPages) * pageSize
	for offset := uint64(0); offset < f.size; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		length := f.size - offset
		if length > chunkMax {
			length = chunkMax
		}
		mapped, err := unix.Mmap(int(f.file.Fd()), int64(offset), int(length), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			return nil, err
		}
		pages := (length + pageSize - 1) / pageSize
		vec := make([]byte, int(pages))
		if err := mincore(mapped, vec); err != nil {
			_ = unix.Munmap(mapped)
			return nil, err
		}
		for i, state := range vec {
			pageOffset := offset + uint64(i)*pageSize
			pageLen := pageSize
			if pageLen > f.size-pageOffset {
				pageLen = f.size - pageOffset
			}
			if state&1 != 0 {
				if active != nil && active.Offset+active.Length == pageOffset {
					active.Length += pageLen
				} else {
					if len(ranges) >= maxRanges {
						_ = unix.Munmap(mapped)
						return nil, fmt.Errorf("hotset exceeds %d resident ranges", maxRanges)
					}
					ranges = append(ranges, Range{Offset: pageOffset, Length: pageLen})
					active = &ranges[len(ranges)-1]
				}
			} else {
				active = nil
			}
		}
		if err := unix.Munmap(mapped); err != nil {
			return nil, err
		}
		offset += length
	}
	return ranges, nil
}

func mincore(mapping []byte, vec []byte) error {
	if len(mapping) == 0 || len(vec) == 0 {
		return errors.New("mincore requires non-empty mapping and vector")
	}
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&mapping[0])), uintptr(len(mapping)), uintptr(unsafe.Pointer(&vec[0])))
	runtime.KeepAlive(mapping)
	runtime.KeepAlive(vec)
	if errno != 0 {
		return errno
	}
	return nil
}
