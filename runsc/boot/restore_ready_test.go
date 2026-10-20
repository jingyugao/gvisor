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

package boot

import (
	"errors"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/sync"
	"gvisor.dev/gvisor/runsc/boot/sentryapi"
)

func awaitRestoreResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("restore waiter did not finish")
		return nil
	}
}

func TestRestoreReadyBeforeAllPages(t *testing.T) {
	l := &Loader{state: restoringStarted}
	l.restoreDone = sync.NewCond(&l.mu)
	cm := &containerManager{l: l}
	ready := make(chan error, 1)
	complete := make(chan error, 1)
	go func() { ready <- l.waitRestoreReady() }()
	go func() { complete <- l.waitRestore() }()
	defer cm.onRestoreDone(sentryapi.Savings{})

	cm.onRestoreReady()
	if err := awaitRestoreResult(t, ready); err != nil {
		t.Fatalf("waitRestoreReady: %v", err)
	}
	select {
	case err := <-complete:
		t.Fatalf("full restore completed before page loading: %v", err)
	default:
	}
	cm.onRestoreDone(sentryapi.Savings{})
	if err := awaitRestoreResult(t, complete); err != nil {
		t.Fatalf("waitRestore: %v", err)
	}
}

func TestRestoreFailureWakesReadinessAndCompletion(t *testing.T) {
	l := &Loader{state: restoringStarted}
	l.restoreDone = sync.NewCond(&l.mu)
	cm := &containerManager{l: l}
	ready := make(chan error, 1)
	complete := make(chan error, 1)
	go func() { ready <- l.waitRestoreReady() }()
	go func() { complete <- l.waitRestore() }()
	want := errors.New("restore failed before application readiness")
	defer cm.onRestoreFailed(want)
	cm.onRestoreFailed(want)
	for _, result := range []<-chan error{ready, complete} {
		if err := awaitRestoreResult(t, result); !errors.Is(err, want) {
			t.Errorf("restore waiter error = %v, want %v", err, want)
		}
	}
}

func TestRestoreReadyRejectsUnrestoredSandbox(t *testing.T) {
	l := &Loader{state: created}
	if err := l.waitRestoreReady(); err == nil {
		t.Fatal("waitRestoreReady succeeded before restore began")
	}
}
