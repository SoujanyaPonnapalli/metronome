// Copyright 2026 The etcd Authors
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

package etcdserver

import (
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/etcdserver/metronome"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// newTestRaftNode builds a minimal raftNode with metronome enabled for
// work-stealing state-machine tests. It does NOT start raft.
func newTestRaftNode(t *testing.T, nodeID uint64, clusterIDs []uint64, wsTimeout, wsDuration time.Duration) *raftNode {
	t.Helper()
	scheme, err := metronome.NewScheme(clusterIDs, 0 /* default f+1 */)
	if err != nil {
		t.Fatalf("NewScheme: %v", err)
	}
	rs := raft.NewMemoryStorage()
	return &raftNode{
		lg: zap.NewNop(),
		raftNodeConfig: raftNodeConfig{
			lg:              zap.NewNop(),
			raftStorage:     rs,
			localID:         nodeID,
			metronomeScheme: scheme,
			wsTimeout:       wsTimeout,
			wsDuration:      wsDuration,
		},
	}
}

// seedMemoryStorage pushes contiguous dummy entries [from..to] into
// raftStorage so collectInMemoryEntries has something to return.
func seedMemoryStorage(t *testing.T, rn *raftNode, from, to uint64) {
	t.Helper()
	ents := make([]raftpb.Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		ents = append(ents, raftpb.Entry{Term: 1, Index: i, Data: []byte("x")})
	}
	if err := rn.raftStorage.Append(ents); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// entries builds a slice of raftpb.Entry with contiguous indices.
func entries(from, to uint64) []raftpb.Entry {
	out := make([]raftpb.Entry, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, raftpb.Entry{Term: 1, Index: i, Data: []byte("x")})
	}
	return out
}

// ----- Basic state-machine tests -------------------------------------

func TestRecordReadySkipped_InitializesBaseline(t *testing.T) {
	rn := newTestRaftNode(t, /*self=*/ 2, []uint64{1, 2, 3}, 50*time.Millisecond, time.Minute)

	rn.recordReadySkipped(
		/*hsCommit=*/ 0,
		/*allEnts=*/ entries(1, 3),
		/*kept=*/ entries(1, 1), // 2 and 3 skipped
	)
	if rn.wsLastAdvance.IsZero() {
		t.Fatalf("expected baseline timestamp to be set on first Ready")
	}
	if len(rn.wsSkipped) != 2 {
		t.Fatalf("expected 2 skipped indices, got %v", rn.wsSkipped)
	}
}

func TestRecordReadySkipped_CommitAdvanceDrainsBuffer(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 50*time.Millisecond, time.Minute)

	// Step 1: skip indices 2, 3, 4.
	rn.recordReadySkipped(0, entries(2, 4), nil)
	if len(rn.wsSkipped) != 3 {
		t.Fatalf("want 3 skipped, got %v", rn.wsSkipped)
	}
	first := rn.wsLastAdvance

	// Sleep a touch so we can observe timestamp advance.
	time.Sleep(2 * time.Millisecond)

	// Step 2: commit advances to 3. Entries 2,3 should be dropped;
	// entry 4 remains in buffer. wsLastAdvance should be refreshed.
	rn.recordReadySkipped(3, nil, nil)
	if got := rn.wsSkipped; len(got) != 1 || got[0] != 4 {
		t.Fatalf("want only [4] remaining, got %v", got)
	}
	if !rn.wsLastAdvance.After(first) {
		t.Fatalf("expected wsLastAdvance to advance on commit progress")
	}
}

// ----- Trigger tests -------------------------------------------------

func TestMaybeTriggerWorkSteal_FiresOnStall(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3},
		/*wsTimeout=*/ 5*time.Millisecond,
		/*wsDuration=*/ 100*time.Millisecond)
	seedMemoryStorage(t, rn, 1, 10)

	// Simulate: skipped a few, cluster has advanced at least once
	// before (wsLastCommit set), commit progress stalled since.
	rn.wsSkipped = []uint64{3, 5, 7}
	rn.wsLastCommit = 2 // real prior commit observed
	rn.wsLastAdvance = time.Now().Add(-50 * time.Millisecond) // stale

	// Inject a no-op storage so Save doesn't panic.
	rn.storage = &noopStorage{}

	// Record before + fire.
	beforeTriggered := counterValue(metronomeWorkStealsTriggered)
	beforeEntries := counterValue(metronomeWorkStealEntries)

	rn.maybeTriggerWorkSteal()

	if !rn.inWorkStealMode() {
		t.Fatalf("expected work-steal mode to be active after trigger")
	}
	if len(rn.wsSkipped) != 0 {
		t.Fatalf("expected skipped buffer drained, got %v", rn.wsSkipped)
	}

	afterTriggered := counterValue(metronomeWorkStealsTriggered)
	afterEntries := counterValue(metronomeWorkStealEntries)
	if afterTriggered != beforeTriggered+1 {
		t.Fatalf("metronome_work_steals_triggered_total should have incremented by 1 (before=%v after=%v)", beforeTriggered, afterTriggered)
	}
	if afterEntries != beforeEntries+3 {
		t.Fatalf("metronome_work_steal_entries_written_total should have increased by 3 (before=%v after=%v)", beforeEntries, afterEntries)
	}
}

// Regression guard: during the initial startup window (before any
// real commit has been observed), the timer can appear "stale"
// because leader election and initial replication take hundreds of
// ms, yet no commit has advanced yet. We must NOT fire WS in that
// window — otherwise every metronome follower immediately enters
// "log everything" mode on startup and the scheme's byte savings
// collapse to zero.
func TestMaybeTriggerWorkSteal_NoFireBeforeFirstCommit(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 1*time.Millisecond, time.Minute)
	seedMemoryStorage(t, rn, 1, 5)
	rn.wsSkipped = []uint64{2, 3}
	// wsLastCommit == 0 simulates "haven't seen a commit advance yet."
	rn.wsLastAdvance = time.Now().Add(-1 * time.Second) // would otherwise trigger
	rn.storage = &noopStorage{}
	before := counterValue(metronomeWorkStealsTriggered)
	rn.maybeTriggerWorkSteal()
	if rn.inWorkStealMode() {
		t.Fatalf("WS must NOT fire before first commit observed (startup guard)")
	}
	if counterValue(metronomeWorkStealsTriggered) != before {
		t.Fatalf("trigger counter should not have moved")
	}
}

func TestMaybeTriggerWorkSteal_NoFireWhenEmpty(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 1*time.Millisecond, time.Minute)
	rn.wsLastAdvance = time.Now().Add(-1 * time.Second)
	// buffer is empty
	before := counterValue(metronomeWorkStealsTriggered)
	rn.maybeTriggerWorkSteal()
	if rn.inWorkStealMode() {
		t.Fatalf("should not enter WS mode with empty buffer")
	}
	if counterValue(metronomeWorkStealsTriggered) != before {
		t.Fatalf("should not have incremented trigger counter")
	}
}

func TestMaybeTriggerWorkSteal_NoFireWithinTimeout(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 1*time.Second, time.Minute)
	rn.wsSkipped = []uint64{5}
	rn.wsLastAdvance = time.Now() // fresh
	rn.storage = &noopStorage{}
	before := counterValue(metronomeWorkStealsTriggered)
	rn.maybeTriggerWorkSteal()
	if rn.inWorkStealMode() {
		t.Fatalf("should not enter WS mode before timeout elapsed")
	}
	if counterValue(metronomeWorkStealsTriggered) != before {
		t.Fatalf("trigger counter should not have moved")
	}
}

func TestMaybeTriggerWorkSteal_ExitsAfterDuration(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3},
		1*time.Millisecond,
		10*time.Millisecond)
	seedMemoryStorage(t, rn, 1, 5)
	rn.wsSkipped = []uint64{2, 3}
	rn.wsLastCommit = 1 // simulate prior commit observed
	rn.wsLastAdvance = time.Now().Add(-1 * time.Second)
	rn.storage = &noopStorage{}

	rn.maybeTriggerWorkSteal()
	if !rn.inWorkStealMode() {
		t.Fatalf("expected WS active")
	}

	// Wait past the WS window.
	time.Sleep(30 * time.Millisecond)

	rn.maybeTriggerWorkSteal()
	if rn.inWorkStealMode() {
		t.Fatalf("expected WS window to have elapsed")
	}
}

// ----- Filter integration test ---------------------------------------

func TestFilterMetronomeEntries_PassthroughInWSMode(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 1*time.Millisecond, time.Second)
	// Force WS mode.
	rn.wsActiveUntil = time.Now().Add(5 * time.Second)

	all := entries(10, 15)
	got := rn.filterMetronomeEntries(all)
	if len(got) != len(all) {
		t.Fatalf("in WS mode, filter should pass through all entries; got %d of %d", len(got), len(all))
	}
}

// ----- Safety property: buffer ordering ------------------------------

// Skipped indices, once tracked, must be drained in order whenever
// commit advances past them. This test exercises that with interleaved
// Readies.
func TestWorkSteal_BufferDrainedInCommitOrder(t *testing.T) {
	rn := newTestRaftNode(t, 2, []uint64{1, 2, 3}, 100*time.Millisecond, time.Minute)

	// Three Readies add indices 2..4, 5..7, 8..10 to buffer (none kept).
	rn.recordReadySkipped(0, entries(2, 4), nil)
	rn.recordReadySkipped(0, entries(5, 7), nil)
	rn.recordReadySkipped(0, entries(8, 10), nil)
	if len(rn.wsSkipped) != 9 {
		t.Fatalf("want 9 buffered, got %d", len(rn.wsSkipped))
	}

	// Commit to 6 → first half should drain.
	rn.recordReadySkipped(6, nil, nil)
	if len(rn.wsSkipped) != 4 {
		t.Fatalf("want 4 buffered after commit=6, got %v", rn.wsSkipped)
	}
	for _, idx := range rn.wsSkipped {
		if idx <= 6 {
			t.Fatalf("index %d still buffered despite commit=6", idx)
		}
	}

	// Commit to 10 → buffer empty.
	rn.recordReadySkipped(10, nil, nil)
	if len(rn.wsSkipped) != 0 {
		t.Fatalf("expected empty buffer after commit=10, got %v", rn.wsSkipped)
	}
}

// ----- helpers -------------------------------------------------------

// noopStorage is a stub serverstorage.Storage for tests. Save accepts
// anything and returns nil.
type noopStorage struct{}

func (*noopStorage) Save(_ raftpb.HardState, _ []raftpb.Entry) error { return nil }
func (*noopStorage) SaveSnap(_ raftpb.Snapshot) error                { return nil }
func (*noopStorage) Close() error                                    { return nil }
func (*noopStorage) Release(_ raftpb.Snapshot) error                 { return nil }
func (*noopStorage) Sync() error                                     { return nil }
func (*noopStorage) MinimalEtcdVersion() *semver.Version             { return nil }

// counterValue returns the current value of a prometheus Counter.
func counterValue(c prometheus.Counter) float64 {
	return testutil.ToFloat64(c)
}
