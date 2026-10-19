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

// Package checkpointprefetch captures host page-cache residency for immutable
// checkpoints and reads selected ranges to warm the host page cache.
package checkpointprefetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	manifestVersion = 1
	maxManifestSize = 64 << 20
	maxRanges       = 100000
	readBufferSize  = 256 * 1024
)

// Supported checkpoint files are the regular files captured by gVisor
// save/restore. Missing files are ignored, allowing checkpoint formats that
// omit empty components. At least one file must exist.
var checkpointFiles = []string{"checkpoint.img", "pages_meta.img", "pages.img"}

// Range is a byte range in a checkpoint file. Capture emits page-aligned
// offsets and lengths, except that the final range may end at EOF.
type Range struct {
	Offset uint64 `json:"offset"`
	Length uint64 `json:"length"`
}

// File records the size and ranges selected for a checkpoint file.
type File struct {
	Name   string  `json:"name"`
	Size   uint64  `json:"size"`
	Ranges []Range `json:"ranges"`
}

// Manifest describes a hotset for one immutable checkpoint. CheckpointID is
// supplied by the orchestrator and must never be reused for different
// checkpoint contents. LayoutDigest fingerprints the sorted file names and
// sizes. MetadataContentDigest hashes pages_meta.img when that file is present;
// this detects same-size metadata replacement without reading all pages.img
// data. When pages_meta.img is absent, identity relies on the caller's
// immutable-ID contract and the file-layout check.
type Manifest struct {
	Version               int    `json:"version"`
	CheckpointID          string `json:"checkpoint_id"`
	PageSize              uint64 `json:"page_size"`
	LayoutDigest          string `json:"layout_digest"`
	MetadataFile          string `json:"metadata_file,omitempty"`
	MetadataContentDigest string `json:"metadata_content_digest,omitempty"`
	Files                 []File `json:"files"`
}

// Capture measures which pages of checkpointDir are resident in the host page
// cache. This is a cache footprint, including pages resident for any reason;
// it does not attribute pages to a guest process or identify guest addresses.
func Capture(ctx context.Context, checkpointID, checkpointDir string) (*Manifest, error) {
	if strings.TrimSpace(checkpointID) == "" {
		return nil, errors.New("checkpoint ID must be non-empty")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pageSize := uint64(os.Getpagesize())
	if pageSize == 0 {
		return nil, errors.New("invalid system page size")
	}
	files, err := openCheckpoint(checkpointDir)
	if err != nil {
		return nil, err
	}
	defer closeFiles(files)
	if len(files) == 0 {
		return nil, errors.New("checkpoint has no supported files")
	}

	manifest := &Manifest{Version: manifestVersion, CheckpointID: checkpointID, PageSize: pageSize}
	for _, f := range files {
		entry := File{Name: f.name, Size: f.size}
		ranges, err := residentRanges(ctx, f, pageSize)
		if err != nil {
			return nil, fmt.Errorf("capture %s: %w", f.name, err)
		}
		entry.Ranges = ranges
		manifest.Files = append(manifest.Files, entry)
	}
	manifest.LayoutDigest = layoutDigest(manifest.Files)
	for _, f := range files {
		if f.name == "pages_meta.img" {
			digest, err := fileContentDigest(ctx, f)
			if err != nil {
				return nil, fmt.Errorf("digest metadata file %s: %w", f.name, err)
			}
			manifest.MetadataFile = f.name
			manifest.MetadataContentDigest = digest
			break
		}
	}
	if err := Validate(manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

// Prefetch reads only ranges listed in manifest, warming the host page cache.
// expectedCheckpointID must be the immutable ID assigned by the caller to the
// checkpoint being restored. A zero rate means unlimited logical read rate.
// Cancellation is checked between bounded reads and during rate-limit waits.
func Prefetch(ctx context.Context, checkpointDir string, manifest *Manifest, expectedCheckpointID string, bytesPerSecond uint64) error {
	if expectedCheckpointID == "" || manifest == nil || manifest.CheckpointID != expectedCheckpointID {
		return errors.New("hotset manifest checkpoint ID does not match")
	}
	if err := Validate(manifest); err != nil {
		return err
	}
	if uint64(manifest.PageSize) != uint64(os.Getpagesize()) {
		return fmt.Errorf("manifest page size %d does not match host page size %d", manifest.PageSize, os.Getpagesize())
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	files, err := openCheckpoint(checkpointDir)
	if err != nil {
		return err
	}
	defer closeFiles(files)
	if err := validateCheckpointFiles(ctx, manifest, files); err != nil {
		return err
	}

	byName := make(map[string]*checkpointFile, len(files))
	for _, f := range files {
		byName[f.name] = f
	}
	buffer := make([]byte, readBufferSize)
	start := time.Now()
	var total uint64
	for _, entry := range manifest.Files {
		f := byName[entry.Name]
		for _, r := range entry.Ranges {
			offset, remaining := r.Offset, r.Length
			for remaining > 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
				amount := uint64(len(buffer))
				if amount > remaining {
					amount = remaining
				}
				n, err := f.file.ReadAt(buffer[:int(amount)], int64(offset))
				if err != nil && !(err == io.EOF && uint64(n) == amount) {
					return fmt.Errorf("read %s at offset %d: %w", f.name, offset, err)
				}
				if uint64(n) != amount {
					return fmt.Errorf("short read %s at offset %d: got %d bytes, want %d", f.name, offset, n, amount)
				}
				offset += amount
				remaining -= amount
				if total > math.MaxUint64-amount {
					return errors.New("total prefetch byte count overflow")
				}
				total += amount
				if bytesPerSecond != 0 {
					// Use cumulative bytes to avoid accumulating per-chunk rounding drift.
					targetNanos := float64(total) / float64(bytesPerSecond) * float64(time.Second)
					if targetNanos > math.MaxInt64 {
						targetNanos = math.MaxInt64
					}
					wait := time.Duration(int64(targetNanos)) - time.Since(start)
					if wait > 0 {
						timer := time.NewTimer(wait)
						select {
						case <-ctx.Done():
							timer.Stop()
							return ctx.Err()
						case <-timer.C:
						}
					}
				}
			}
		}
	}
	return nil
}

// Validate checks manifest structure, ordering, bounds, and its metadata
// digest. It performs no I/O.
func Validate(m *Manifest) error {
	if m == nil {
		return errors.New("hotset manifest is nil")
	}
	if m.Version != manifestVersion {
		return fmt.Errorf("unsupported hotset manifest version %d", m.Version)
	}
	if strings.TrimSpace(m.CheckpointID) == "" {
		return errors.New("hotset manifest checkpoint ID is empty")
	}
	if m.PageSize == 0 {
		return errors.New("hotset manifest page size is zero")
	}
	if len(m.Files) == 0 || len(m.Files) > len(checkpointFiles) {
		return errors.New("hotset manifest has invalid file count")
	}
	known := make(map[string]bool, len(checkpointFiles))
	for _, name := range checkpointFiles {
		known[name] = true
	}
	seen := make(map[string]bool, len(m.Files))
	totalRanges := 0
	for _, f := range m.Files {
		if !known[f.Name] || seen[f.Name] {
			return fmt.Errorf("unsupported or duplicate checkpoint file %q", f.Name)
		}
		seen[f.Name] = true
		var lastEnd uint64
		for _, r := range f.Ranges {
			totalRanges++
			if totalRanges > maxRanges {
				return fmt.Errorf("hotset manifest has more than %d ranges", maxRanges)
			}
			if r.Offset%m.PageSize != 0 || r.Length == 0 || r.Offset > math.MaxUint64-r.Length {
				return fmt.Errorf("invalid range in %s: offset must be page aligned, length positive, and end non-overflowing", f.Name)
			}
			end := r.Offset + r.Length
			if end > f.Size {
				return fmt.Errorf("range in %s ends at %d beyond file size %d", f.Name, end, f.Size)
			}
			if r.Offset < lastEnd {
				return fmt.Errorf("ranges in %s are unsorted or overlap", f.Name)
			}
			lastEnd = end
		}
	}
	if digest := layoutDigest(m.Files); digest != m.LayoutDigest {
		return errors.New("hotset manifest layout digest is invalid")
	}
	metadataEntry := false
	for _, f := range m.Files {
		if f.Name == "pages_meta.img" {
			metadataEntry = true
		}
	}
	if metadataEntry {
		if m.MetadataFile != "pages_meta.img" || len(m.MetadataContentDigest) != sha256.Size*2 {
			return errors.New("hotset manifest must include the pages_meta.img content digest")
		}
		if _, err := hex.DecodeString(m.MetadataContentDigest); err != nil {
			return errors.New("hotset manifest metadata content digest is malformed")
		}
	} else if m.MetadataFile != "" || m.MetadataContentDigest != "" {
		return errors.New("hotset manifest has a metadata content digest without pages_meta.img")
	}
	return nil
}

// ReadManifest loads a bounded JSON manifest and rejects trailing JSON values.
func ReadManifest(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxManifestSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxManifestSize {
		return nil, fmt.Errorf("manifest exceeds %d bytes", maxManifestSize)
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("manifest contains trailing JSON value")
		}
		return nil, fmt.Errorf("invalid trailing manifest data: %w", err)
	}
	if err := Validate(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// WriteManifest atomically writes a validated JSON manifest. The checkpoint
// files are opened read-only by Capture and Prefetch; only this sidecar changes.
func WriteManifest(path string, m *Manifest) error {
	if isCheckpointFileName(filepath.Base(path)) {
		return fmt.Errorf("manifest output %q uses a reserved checkpoint filename", filepath.Base(path))
	}
	if err := Validate(m); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".hotset-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func isCheckpointFileName(name string) bool {
	for _, checkpointName := range checkpointFiles {
		if name == checkpointName {
			return true
		}
	}
	return false
}

type checkpointFile struct {
	name string
	file *os.File
	size uint64
}

func openCheckpoint(dir string) ([]*checkpointFile, error) {
	var files []*checkpointFile
	for _, name := range checkpointFiles {
		path := filepath.Join(dir, name)
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				continue
			}
			closeFiles(files)
			return nil, fmt.Errorf("open checkpoint file %s: %w", name, err)
		}
		file := os.NewFile(uintptr(fd), path)
		st, err := file.Stat()
		if err != nil {
			file.Close()
			closeFiles(files)
			return nil, fmt.Errorf("stat checkpoint file %s: %w", name, err)
		}
		if !st.Mode().IsRegular() || st.Size() < 0 {
			file.Close()
			closeFiles(files)
			return nil, fmt.Errorf("checkpoint path %s is not a regular file", name)
		}
		files = append(files, &checkpointFile{name: name, file: file, size: uint64(st.Size())})
	}
	return files, nil
}

func closeFiles(files []*checkpointFile) {
	for _, f := range files {
		_ = f.file.Close()
	}
}

func validateCheckpointFiles(ctx context.Context, m *Manifest, current []*checkpointFile) error {
	if len(current) != len(m.Files) {
		return errors.New("checkpoint file set does not match hotset manifest")
	}
	byName := make(map[string]*checkpointFile, len(current))
	for _, f := range current {
		byName[f.name] = f
	}
	for _, expected := range m.Files {
		f, ok := byName[expected.Name]
		if !ok || f.size != expected.Size {
			return fmt.Errorf("checkpoint file %s is missing or has changed size", expected.Name)
		}
	}
	// Validate the layout digest using the actual opened files, before reading
	// prefetch ranges. This does not read checkpoint content.
	actual := make([]File, 0, len(current))
	for _, f := range current {
		actual = append(actual, File{Name: f.name, Size: f.size})
	}
	if layoutDigest(actual) != m.LayoutDigest {
		return errors.New("checkpoint file layout does not match hotset manifest")
	}
	if m.MetadataFile != "" {
		f := byName[m.MetadataFile]
		digest, err := fileContentDigest(ctx, f)
		if err != nil {
			return fmt.Errorf("digest checkpoint metadata file %s: %w", f.name, err)
		}
		if digest != m.MetadataContentDigest {
			return errors.New("checkpoint metadata file content does not match hotset manifest")
		}
	}
	return nil
}

func layoutDigest(files []File) string {
	ordered := append([]File(nil), files...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	h := sha256.New()
	for _, f := range ordered {
		fmt.Fprintf(h, "%s\x00%d\n", f.Name, f.Size)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func fileContentDigest(ctx context.Context, f *checkpointFile) (string, error) {
	h := sha256.New()
	buffer := make([]byte, readBufferSize)
	for offset := uint64(0); offset < f.size; {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		amount := uint64(len(buffer))
		if amount > f.size-offset {
			amount = f.size - offset
		}
		n, err := f.file.ReadAt(buffer[:int(amount)], int64(offset))
		if err != nil && !(err == io.EOF && uint64(n) == amount) {
			return "", fmt.Errorf("read at offset %d: %w", offset, err)
		}
		if uint64(n) != amount {
			return "", fmt.Errorf("short read at offset %d: got %d bytes, want %d", offset, n, amount)
		}
		_, _ = h.Write(buffer[:n])
		offset += amount
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
