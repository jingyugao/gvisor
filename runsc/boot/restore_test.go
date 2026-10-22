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
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"

	"gvisor.dev/gvisor/pkg/abi/linux"
	"gvisor.dev/gvisor/pkg/sentry/kernel"
	"gvisor.dev/gvisor/pkg/sync"
	"gvisor.dev/gvisor/pkg/urpc"
	"gvisor.dev/gvisor/runsc/boot/sentryapi"
	"gvisor.dev/gvisor/runsc/config"
)

// notifyingLocker signals when Cond.Wait releases the lock. This lets tests
// synchronize with a state-change waiter without sleeps or scheduler
// assumptions.
type notifyingLocker struct {
	mu      sync.Locker
	waiting chan struct{}
}

func (l *notifyingLocker) Lock() {
	l.mu.Lock()
}

func (l *notifyingLocker) Unlock() {
	l.mu.Unlock()
	select {
	case l.waiting <- struct{}{}:
	default:
	}
}

func waitForRestoreWaiters(t *testing.T, locker *notifyingLocker, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-locker.waiting:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d restore requests waited for checkpoint initialization", i, count)
		}
	}
}

func newRestoreWaitTestManager(state loaderState) (*containerManager, *notifyingLocker) {
	l := &Loader{state: state, noRootContainer: noRootContainer{enabled: true}}
	locker := &notifyingLocker{mu: &l.mu, waiting: make(chan struct{}, 16)}
	l.stateChanged.L = locker
	return &containerManager{l: l}, locker
}

func publishRestoreState(cm *containerManager, state loaderState, err error) {
	cm.l.mu.Lock()
	cm.l.state = state
	cm.l.restoreErr = err
	cm.l.stateChanged.Broadcast()
	cm.l.mu.Unlock()
}

func TestRestoreWaitsForCheckpointInitializationAndReusesPayload(t *testing.T) {
	cm, locker := newRestoreWaitTestManager(loadingCheckpoint)
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	result := make(chan error, 1)
	go func() {
		result <- cm.Restore(&sentryapi.RestoreOpts{
			FromSubcontainer: true,
			FilePayload:      urpc.FilePayload{Files: []*os.File{r}},
		}, nil)
	}()
	waitForRestoreWaiters(t, locker, 1)

	// Once checkpoint initialization publishes restoringUnstarted, this
	// subcontainer reuses the shared restorer. Its checkpoint FDs are unused.
	publishRestoreState(cm, restoringUnstarted, nil)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Restore() = %v, want nil after initialization completes", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Restore() remained blocked after initialization completed")
	}
	if _, err := r.Stat(); err != nil {
		t.Fatalf("unused checkpoint payload was consumed: %v", err)
	}
}

func TestRestoreHandlersWaitForCheckpointFailure(t *testing.T) {
	wantErr := errors.New("checkpoint initialization failed")
	calls := []struct {
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
			name: "StartSubcontainer",
			call: func(cm *containerManager) error {
				return cm.StartSubcontainer(&sentryapi.StartArgs{
					Spec: &specs.Spec{},
					Conf: &config.Config{},
					CID:  "business",
				}, nil)
			},
		},
		{
			name: "RestoreSubcontainer",
			call: func(cm *containerManager) error {
				return cm.RestoreSubcontainer(&sentryapi.StartArgs{}, nil)
			},
		},
	}
	cm, locker := newRestoreWaitTestManager(loadingCheckpoint)
	results := make(chan error, len(calls))
	for _, tc := range calls {
		tc := tc
		go func() { results <- tc.call(cm) }()
	}
	waitForRestoreWaiters(t, locker, len(calls))

	// All entry points must observe the one terminal initialization failure.
	publishRestoreState(cm, restoreFailed, wantErr)
	for _, tc := range calls {
		select {
		case err := <-results:
			if err != wantErr {
				t.Errorf("handler error = %v, want original initialization error %v", err, wantErr)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s remained blocked after initialization failed", tc.name)
		}
	}
}

func TestRestoreInitializationFailureIsTerminal(t *testing.T) {
	// A split restore cannot initialize when no filesystem restore files were
	// donated at sandbox creation. The first failure is terminal and cached in
	// Loader state for later Restore RPCs.
	l := &Loader{state: created, noRootContainer: noRootContainer{enabled: true}}
	cm := &containerManager{l: l}
	err := cm.Restore(&sentryapi.RestoreOpts{
		SplitFSRestore: true,
		FilePayload:    urpc.FilePayload{Files: []*os.File{os.Stdin}},
	}, nil)
	if err == nil {
		t.Fatal("Restore() succeeded without split filesystem restore files")
	}
	if l.state != restoreFailed || l.restoreErr == nil || !errors.Is(l.restoreErr, err) {
		t.Fatalf("failure not published: state=%s, error=%v", l.state, l.restoreErr)
	}
	if got := cm.Restore(&sentryapi.RestoreOpts{}, nil); got != l.restoreErr {
		t.Fatalf("repeated Restore() = %v, want terminal error %v", got, l.restoreErr)
	}
	if got := cm.RestoreSubcontainer(&sentryapi.StartArgs{}, nil); got != l.restoreErr {
		t.Fatalf("RestoreSubcontainer() = %v, want terminal error %v", got, l.restoreErr)
	}
}

func TestWaitRestoreReturnsWhenSandboxStops(t *testing.T) {
	for _, state := range []loaderState{ready, loadingCheckpoint, restoringUnstarted, restoringStarted} {
		t.Run(state.String(), func(t *testing.T) {
			cm, locker := newRestoreWaitTestManager(state)
			result := make(chan error, 1)
			go func() { result <- cm.l.waitRestore() }()
			waitForRestoreWaiters(t, locker, 1)

			cm.l.requestStop()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("waitRestore() succeeded after sandbox stopped")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("waitRestore() remained blocked after sandbox stopped")
			}
		})
	}
}

func TestRestoreShutdownWaitsForKernelReplacement(t *testing.T) {
	l, cleanup, err := createLoader(testConfig(), testSpec())
	if err != nil {
		t.Fatalf("createLoader() failed: %v", err)
	}
	defer cleanup()
	defer l.Destroy()

	l.noRootContainer.enabled = true
	l.mu.Lock()
	l.state = restoringUnstarted
	l.mu.Unlock()

	// Simulate the interval where checkpoint restore has replaced l.k with a
	// not-yet-usable Kernel. WaitExit must take kernelMu before it dereferences
	// l.k, just like all checkpoint replacement operations.
	l.kernelMu.Lock()
	oldKernel := l.k
	l.k = &kernel.Kernel{}
	kernelLocked := true
	defer func() {
		if kernelLocked {
			l.k = oldKernel
			l.kernelMu.Unlock()
		}
	}()

	stoppedState := make(chan struct{})
	go func() {
		l.mu.Lock()
		for l.state != stopping {
			l.stateChanged.Wait()
		}
		l.mu.Unlock()
		close(stoppedState)
	}()

	l.noRootContainer.stop()
	type waitExitResult struct {
		status linux.WaitStatus
		panic  any
	}
	done := make(chan waitExitResult, 1)
	go func() {
		var result waitExitResult
		defer func() {
			result.panic = recover()
			done <- result
		}()
		result.status = l.WaitExit()
	}()

	select {
	case <-stoppedState:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitExit() did not request stop")
	}
	select {
	case result := <-done:
		t.Fatalf("WaitExit() completed before kernel replacement finished: %+v", result)
	default:
	}

	// Always restore l.k before releasing the lock, including on test failure.
	l.k = oldKernel
	l.kernelMu.Unlock()
	kernelLocked = false
	select {
	case result := <-done:
		if result.panic != nil {
			t.Fatalf("WaitExit() panicked after kernel replacement: %v", result.panic)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitExit() did not finish after kernel replacement")
	}
}

func TestStartSubcontainerRechecksCheckpointStateUnderLock(t *testing.T) {
	originalTG := &kernel.ThreadGroup{}
	for _, tc := range []struct {
		name            string
		state           loaderState
		tg              *kernel.ThreadGroup
		wantErr         error
		wantErrorString string
	}{
		{
			name:    "loadingCheckpoint",
			state:   loadingCheckpoint,
			wantErr: errRestoreSubcontainer,
		},
		{
			name:    "restoringUnstarted",
			state:   restoringUnstarted,
			wantErr: errRestoreSubcontainer,
		},
		{
			name:            "restoredWithExistingThreadGroup",
			state:           restored,
			tg:              originalTG,
			wantErrorString: "already started",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ep := &execProcess{tg: tc.tg}
			l := &Loader{
				sandboxID: "sandbox",
				state:     tc.state,
				processes: map[execID]*execProcess{
					{cid: "sub"}: ep,
				},
				noRootContainer: noRootContainer{enabled: true},
				failedToStart:   make(map[string]struct{}),
			}
			err := l.startSubcontainer(&specs.Spec{}, &config.Config{}, "sub", nil, nil, nil, nil, nil, nil)
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("startSubcontainer() = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErrorString != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrorString)) {
				t.Fatalf("startSubcontainer() = %v, want error containing %q", err, tc.wantErrorString)
			}
			if tc.wantErr == nil && tc.wantErrorString == "" && err != nil {
				t.Fatalf("startSubcontainer() = %v, want nil", err)
			}
			if tc.state == restored && ep.tg != originalTG {
				t.Fatalf("startSubcontainer() changed restored thread group: got %p, want %p", ep.tg, originalTG)
			}
			if _, marked := l.failedToStart["sub"]; marked {
				t.Fatal("startSubcontainer() marked an already started container as failed")
			}
			if got := len(l.processes); got != 1 {
				t.Fatalf("startSubcontainer() changed process registration: got %d entries, want 1", got)
			}
		})
	}
}

func TestRestoreContainerInfoRejectsTerminalLoaderStates(t *testing.T) {
	failedErr := errors.New("restore already failed")
	for _, tc := range []struct {
		name       string
		state      loaderState
		restoreErr error
	}{
		{name: "restored", state: restored},
		{name: "stopping", state: stopping},
		{name: "restoreFailed", state: restoreFailed, restoreErr: failedErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &Loader{state: tc.state, restoreErr: tc.restoreErr, noRootContainer: noRootContainer{enabled: true}}
			original := &containerInfo{cid: "original", containerName: "original"}
			r := &restorer{
				totalContainers: 2,
				containers:      []*containerInfo{original},
				// A terminal restorer has already released its timing timer.
				timer: nil,
			}
			err := r.restoreContainerInfo(l, &containerInfo{cid: "late", containerName: "late"})
			if err == nil {
				t.Fatal("restoreContainerInfo() succeeded in terminal loader state")
			}
			if tc.restoreErr != nil && err != tc.restoreErr {
				t.Fatalf("restoreContainerInfo() = %v, want cached error %v", err, tc.restoreErr)
			}
			if got := len(r.containers); got != 1 || r.containers[0] != original {
				t.Fatalf("restoreContainerInfo() changed containers: got %v, want only original entry", r.containers)
			}
		})
	}
}

func TestOnRestoreFailedIgnoresTerminalLoaderStates(t *testing.T) {
	for _, tc := range []struct {
		name       string
		state      loaderState
		restoreErr error
	}{
		{name: "restored", state: restored},
		{name: "stopping", state: stopping, restoreErr: errors.New("existing stop error")},
		{name: "restoreFailed", state: restoreFailed, restoreErr: errors.New("original restore failure")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &Loader{state: tc.state, restoreErr: tc.restoreErr, noRootContainer: noRootContainer{enabled: true}}
			r := &restorer{}
			cm := &containerManager{l: l, restorer: r}
			cm.onRestoreFailed(errors.New("late restore failure"))
			if l.state != tc.state {
				t.Fatalf("onRestoreFailed() changed state to %s, want %s", l.state, tc.state)
			}
			if l.restoreErr != tc.restoreErr {
				t.Fatalf("onRestoreFailed() changed restore error to %v, want %v", l.restoreErr, tc.restoreErr)
			}
			if cm.restorer != r {
				t.Fatal("onRestoreFailed() cleared the existing restorer")
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
