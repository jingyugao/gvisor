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

// checkpointprefetch captures or applies a checkpoint hotset manifest.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gvisor.dev/gvisor/tools/checkpointprefetch"
)

func main() {
	mode := flag.String("mode", "", "operation: capture or prefetch")
	dir := flag.String("checkpoint-dir", "", "directory containing checkpoint.img, pages_meta.img, and/or pages.img")
	id := flag.String("checkpoint-id", "", "caller-assigned immutable checkpoint ID; never reuse for different contents")
	manifestPath := flag.String("manifest", "", "hotset JSON manifest path")
	rate := flag.Uint64("rate-bytes-per-second", 0, "logical prefetch read rate; 0 means unlimited")
	flag.Parse()
	if flag.NArg() != 0 {
		usage()
	}
	if *dir == "" || *id == "" || *manifestPath == "" {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch *mode {
	case "capture":
		manifest, err := checkpointprefetch.Capture(ctx, *id, *dir)
		if err == nil {
			err = checkpointprefetch.WriteManifest(*manifestPath, manifest)
		}
		fatalIf(err)
		fmt.Printf("captured %d files for checkpoint %q; ranges are the current host page-cache footprint\n", len(manifest.Files), manifest.CheckpointID)
	case "prefetch":
		manifest, err := checkpointprefetch.ReadManifest(*manifestPath)
		fatalIf(err)
		fatalIf(checkpointprefetch.Prefetch(ctx, *dir, manifest, *id, *rate))
		fmt.Printf("prefetched selected ranges for checkpoint %q\n", *id)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: checkpointprefetch -mode capture|prefetch -checkpoint-dir DIR -checkpoint-id IMMUTABLE_ID -manifest FILE [-rate-bytes-per-second N]\n")
	os.Exit(2)
}

func fatalIf(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "checkpointprefetch:", err)
		os.Exit(1)
	}
}
