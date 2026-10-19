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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestValidateRejectsInvalidRangesAndMetadata(t *testing.T) {
	base := Manifest{
		Version:      manifestVersion,
		CheckpointID: "checkpoint-a",
		PageSize:     uint64(os.Getpagesize()),
		Files:        []File{{Name: "pages.img", Size: uint64(3 * os.Getpagesize()), Ranges: []Range{{Offset: 0, Length: uint64(os.Getpagesize())}}}},
	}
	base.LayoutDigest = layoutDigest(base.Files)
	tests := []struct {
		name   string
		mutate func(*Manifest)
	}{
		{name: "unaligned", mutate: func(m *Manifest) { m.Files[0].Ranges[0].Offset++ }},
		{name: "out of bounds", mutate: func(m *Manifest) { m.Files[0].Ranges[0].Length = uint64(4 * os.Getpagesize()) }},
		{name: "overlap", mutate: func(m *Manifest) {
			m.Files[0].Ranges = append(m.Files[0].Ranges, Range{Offset: 0, Length: uint64(os.Getpagesize())})
		}},
		{name: "wrong layout digest", mutate: func(m *Manifest) { m.LayoutDigest = "wrong" }},
		{name: "unsupported file", mutate: func(m *Manifest) { m.Files[0].Name = "../pages.img" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := cloneManifest(base)
			test.mutate(m)
			if err := Validate(m); err == nil {
				t.Fatal("Validate succeeded for invalid manifest")
			}
		})
	}
}

func TestPrefetchWarmsOnlySelectedRangeAndPreservesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pages.img")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := os.Getpagesize()
	if err := file.Truncate(int64(4 * pageSize)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := unix.Fadvise(int(file.Fd()), 0, 4*int64(pageSize), unix.FADV_DONTNEED); err != nil {
		file.Close()
		t.Skipf("filesystem does not support FADV_DONTNEED: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	manifest := &Manifest{
		Version:      manifestVersion,
		CheckpointID: "immutable-checkpoint-a",
		PageSize:     uint64(pageSize),
		Files: []File{{
			Name:   "pages.img",
			Size:   uint64(4 * pageSize),
			Ranges: []Range{{Offset: uint64(pageSize), Length: uint64(pageSize)}},
		}},
	}
	manifest.LayoutDigest = layoutDigest(manifest.Files)
	if err := Prefetch(context.Background(), dir, manifest, manifest.CheckpointID, 0); err != nil {
		t.Fatal(err)
	}

	file, err = os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 4*pageSize {
		t.Fatalf("file length changed: got %d, want %d", len(data), 4*pageSize)
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("checkpoint contents changed at byte %d: got %#x, want zero", i, b)
		}
	}
	if err := unix.Fadvise(int(file.Fd()), 0, 4*int64(pageSize), unix.FADV_DONTNEED); err != nil {
		t.Skipf("filesystem does not support FADV_DONTNEED after verification: %v", err)
	}
	// Re-prefetch after read-back re-populated the cache; the test below checks
	// that the selected range itself was brought in by Prefetch.
	if err := Prefetch(context.Background(), dir, manifest, manifest.CheckpointID, 0); err != nil {
		t.Fatal(err)
	}
	resident := residency(t, file, 0, 4)
	if !resident[1] {
		t.Fatalf("selected page is not resident: %v", resident)
	}
	if resident[0] || resident[2] || resident[3] {
		t.Fatalf("unselected pages became resident: %v", resident)
	}
}

func TestPrefetchRejectsIdentityAndSizeMismatchBeforeRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pages.img")
	if err := os.WriteFile(path, make([]byte, 2*os.Getpagesize()), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := &Manifest{
		Version:      manifestVersion,
		CheckpointID: "checkpoint-original",
		PageSize:     uint64(os.Getpagesize()),
		Files:        []File{{Name: "pages.img", Size: uint64(2 * os.Getpagesize()), Ranges: []Range{{Offset: 0, Length: uint64(os.Getpagesize())}}}},
	}
	manifest.LayoutDigest = layoutDigest(manifest.Files)
	if err := Prefetch(context.Background(), dir, manifest, "checkpoint-other", 0); err == nil {
		t.Fatal("Prefetch succeeded for mismatched checkpoint ID")
	}
	if err := os.Truncate(path, int64(3*os.Getpagesize())); err != nil {
		t.Fatal(err)
	}
	if err := Prefetch(context.Background(), dir, manifest, manifest.CheckpointID, 0); err == nil {
		t.Fatal("Prefetch succeeded for mismatched file size")
	}
}

func TestPrefetchHonorsCancellation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pages.img")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(16 * 1024 * 1024); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	manifest := &Manifest{
		Version:      manifestVersion,
		CheckpointID: "checkpoint-cancel",
		PageSize:     uint64(os.Getpagesize()),
		Files:        []File{{Name: "pages.img", Size: 16 * 1024 * 1024, Ranges: []Range{{Offset: 0, Length: 16 * 1024 * 1024}}}},
	}
	manifest.LayoutDigest = layoutDigest(manifest.Files)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = Prefetch(ctx, dir, manifest, manifest.CheckpointID, 1)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("Prefetch error = %v, want context cancellation", err)
	}
}

func TestPrefetchRejectsSameSizeMetadataReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pages_meta.img")
	original := []byte("metadata version A")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	contentDigest := sha256.Sum256(original)
	manifest := &Manifest{
		Version:               manifestVersion,
		CheckpointID:          "immutable-checkpoint-meta",
		PageSize:              uint64(os.Getpagesize()),
		MetadataFile:          "pages_meta.img",
		MetadataContentDigest: hex.EncodeToString(contentDigest[:]),
		Files:                 []File{{Name: "pages_meta.img", Size: uint64(len(original))}},
	}
	manifest.LayoutDigest = layoutDigest(manifest.Files)
	replacement := []byte("metadata version B")
	if len(replacement) != len(original) {
		t.Fatalf("test setup requires same-size metadata, got %d and %d", len(replacement), len(original))
	}
	if err := os.WriteFile(path, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Prefetch(context.Background(), dir, manifest, manifest.CheckpointID, 0); err == nil {
		t.Fatal("Prefetch succeeded for same-size replacement metadata")
	}
}

func TestCaptureSnapshotsRangesBeforeMetadataDigest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pages_meta.img")
	if err := os.WriteFile(path, make([]byte, os.Getpagesize()), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := unix.Fadvise(int(file.Fd()), 0, int64(os.Getpagesize()), unix.FADV_DONTNEED); err != nil {
		t.Skipf("filesystem does not support FADV_DONTNEED: %v", err)
	}
	if resident := residency(t, file, 0, 1); resident[0] {
		t.Skip("filesystem kept test page resident after FADV_DONTNEED")
	}

	manifest, err := Capture(context.Background(), "immutable-checkpoint-capture", dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 1 || len(manifest.Files[0].Ranges) != 0 {
		t.Fatalf("capture included pages warmed by the later metadata digest: %+v", manifest.Files)
	}
	if manifest.MetadataFile != "pages_meta.img" || manifest.MetadataContentDigest == "" {
		t.Fatalf("capture omitted metadata content identity: %+v", manifest)
	}
}

func TestWriteManifestRejectsCheckpointFilenameWithoutChangingSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pages.img")
	original := []byte("checkpoint bytes must remain untouched")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := &Manifest{
		Version:      manifestVersion,
		CheckpointID: "immutable-checkpoint-write-protection",
		PageSize:     uint64(os.Getpagesize()),
		Files:        []File{{Name: "pages.img", Size: uint64(len(original))}},
	}
	manifest.LayoutDigest = layoutDigest(manifest.Files)
	if err := WriteManifest(path, manifest); err == nil {
		t.Fatal("WriteManifest succeeded with a reserved checkpoint filename")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("checkpoint file changed: got %q, want %q", got, original)
	}
}

func residency(t *testing.T, file *os.File, offset uint64, pages int) []bool {
	t.Helper()
	length := pages * os.Getpagesize()
	mapped, err := unix.Mmap(int(file.Fd()), int64(offset), length, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(mapped)
	vec := make([]byte, pages)
	if err := mincore(mapped, vec); err != nil {
		t.Fatal(err)
	}
	resident := make([]bool, pages)
	for i, state := range vec {
		resident[i] = state&1 != 0
	}
	return resident
}

func cloneManifest(m Manifest) *Manifest {
	clone := m
	clone.Files = append([]File(nil), m.Files...)
	for i := range clone.Files {
		clone.Files[i].Ranges = append([]Range(nil), clone.Files[i].Ranges...)
	}
	return &clone
}
