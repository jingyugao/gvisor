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
	"math"
	"os"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/sync"
	"gvisor.dev/gvisor/pkg/urpc"
	"gvisor.dev/gvisor/runsc/boot/sentryapi"
)

// notifyingLocker signals when Cond.Wait releases the lock. This lets tests
// synchronize with the waiter without sleeps or scheduler assumptions.
type notifyingLocker struct {
	mu      sync.Locker
	waiting chan struct{}
	once    sync.Once
}

func (l *notifyingLocker) Lock() {
	l.mu.Lock()
}

func (l *notifyingLocker) Unlock() {
	l.mu.Unlock()
	l.once.Do(func() { close(l.waiting) })
}

func waitForRestoreWaiter(t *testing.T, locker *notifyingLocker) {
	t.Helper()
	select {
	case <-locker.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("restore request did not wait for checkpoint initialization")
	}
}

func newRestoreWaitTestManager() (*containerManager, *notifyingLocker) {
	l := &Loader{state: restoringUnstarted}
	locker := &notifyingLocker{mu: &l.mu, waiting: make(chan struct{})}
	l.restoreDone = sync.NewCond(locker)
	return &containerManager{l: l, restoreInitializing: true}, locker
}

func broadcastRestoreState(cm *containerManager, state loaderState, err error) {
	cm.l.mu.Lock()
	cm.restoreInitializing = false
	cm.l.state = state
	cm.l.restoreErr = err
	cm.l.restoreDone.Broadcast()
	cm.l.mu.Unlock()
}

func TestRestoreWaitsForCheckpointInitialization(t *testing.T) {
	cm, locker := newRestoreWaitTestManager()
	cm.l.noRootContainer.enabled = true
	result := make(chan error, 1)
	go func() {
		result <- cm.Restore(&sentryapi.RestoreOpts{
			FromSubcontainer: true,
			FilePayload:      urpc.FilePayload{Files: []*os.File{os.Stdin}},
		}, nil)
	}()
	waitForRestoreWaiter(t, locker)

	// The second business container reuses the checkpoint initialized by the
	// first one, so its checkpoint payload is intentionally not read.
	broadcastRestoreState(cm, restoringUnstarted, nil)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Restore() = %v, want nil after initialization completes", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Restore() remained blocked after initialization completed")
	}
}

func TestRestoreWaitReturnsInitializationFailure(t *testing.T) {
	wantErr := errors.New("checkpoint initialization failed")
	for _, tc := range []struct {
		name string
		call func(*containerManager) error
	}{
		{
			name: "Restore",
			call: func(cm *containerManager) error {
				return cm.Restore(&sentryapi.RestoreOpts{}, nil)
			},
		},
		{
			name: "RestoreSubcontainer",
			call: func(cm *containerManager) error {
				return cm.RestoreSubcontainer(&sentryapi.StartArgs{}, nil)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cm, locker := newRestoreWaitTestManager()
			result := make(chan error, 1)
			go func() { result <- tc.call(cm) }()
			waitForRestoreWaiter(t, locker)

			broadcastRestoreState(cm, restoreFailed, wantErr)
			select {
			case err := <-result:
				if err != wantErr {
					t.Fatalf("handler error = %v, want original initialization error %v", err, wantErr)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("handler remained blocked after initialization failed")
			}
		})
	}
}

func TestTSCOffsetFromSnapshot(t *testing.T) {
	const (
		ghz3     = uint64(3_000_000_000)
		nsPerDay = uint64(86_400_000_000_000)
	)
	for _, tc := range []struct {
		name         string
		baseCycle    uint64
		baseFreq     uint64
		baseRealtime uint64
		curCycle     uint64
		curRealtime  uint64
		want         uint64
		wantOK       bool
	}{
		{
			name:         "NoTimeElapsed",
			baseCycle:    1000,
			baseFreq:     ghz3,
			baseRealtime: 5_000,
			curCycle:     400,
			curRealtime:  5_000,
			want:         600,
			wantOK:       true,
		},
		{
			name:         "RealtimeBehindSnapshot",
			baseCycle:    1000,
			baseFreq:     ghz3,
			baseRealtime: 10_000,
			curCycle:     400,
			curRealtime:  5_000,
			want:         600,
			wantOK:       true,
		},
		{
			name:         "OneSecond",
			baseCycle:    1000,
			baseFreq:     ghz3,
			baseRealtime: 0,
			curCycle:     1000,
			curRealtime:  1_000_000_000,
			want:         ghz3,
			wantOK:       true,
		},
		{
			// deltaNs * baseFreq overflows 64 bits here.
			name:         "OneDay",
			baseCycle:    1_000_000,
			baseFreq:     ghz3,
			baseRealtime: 1_000,
			curCycle:     1_000_000,
			curRealtime:  1_000 + nsPerDay,
			want:         86_400 * ghz3,
			wantOK:       true,
		},
		{
			name:         "NegativeOffset",
			baseCycle:    1000,
			baseFreq:     ghz3,
			baseRealtime: 0,
			curCycle:     5000,
			curRealtime:  0,
			want:         ^uint64(4000) + 1,
			wantOK:       true,
		},
		{
			name:         "ElapsedCyclesOverflow",
			baseCycle:    0,
			baseFreq:     math.MaxUint64,
			baseRealtime: 0,
			curCycle:     0,
			curRealtime:  2_000_000_000,
			wantOK:       false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := tscOffsetFromSnapshot(tc.baseCycle, tc.baseFreq, tc.baseRealtime, tc.curCycle, tc.curRealtime)
			if ok != tc.wantOK {
				t.Fatalf("tscOffsetFromSnapshot() ok = %t, want %t", ok, tc.wantOK)
			}
			if ok && got != tc.want {
				t.Errorf("tscOffsetFromSnapshot() = %d, want %d", got, tc.want)
			}
		})
	}
}
