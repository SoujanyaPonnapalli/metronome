// Copyright 2015 The etcd Authors
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
	"expvar"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/metronome"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

var (
	// protects raftStatus
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
	expvar.Publish("raft.status", expvar.Func(func() any {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()
		if raftStatus == nil {
			return nil
		}
		return raftStatus()
	}))
}

// toApply contains entries, snapshot to be applied. Once
// an toApply is consumed, the entries will be persisted to
// raft storage concurrently; the application must read
// notifyc before assuming the raft messages are stable.
type toApply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	// notifyc synchronizes etcd server applies with the raft node
	notifyc chan struct{}
	// raftAdvancedC notifies EtcdServer.apply that
	// 'raftLog.applied' has advanced by r.Advance
	// it should be used only when entries contain raftpb.EntryConfChange
	raftAdvancedC <-chan struct{}
}

type raftNode struct {
	lg *zap.Logger

	tickMu *sync.RWMutex
	// timestamp of the latest tick
	latestTickTs time.Time
	raftNodeConfig

	// metronomeSchemeLive holds the currently active metronome scheme.
	// It is read (without a lock) on the Ready hot path and is swapped
	// when membership changes (via UpdateMetronomeScheme, called from
	// the apply loop after a ConfChange is committed).
	metronomeSchemeLive atomic.Pointer[metronome.Scheme]

	// a chan to send/receive snapshot
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	applyc chan toApply

	// a chan to send out readState
	readStateC chan raft.ReadState

	// utility
	ticker *time.Ticker
	// contention detectors for raft heartbeat message
	td *contention.TimeoutDetector

	stopped chan struct{}
	done    chan struct{}
}

type raftNodeConfig struct {
	lg *zap.Logger

	// to check if msg receiver is removed from cluster
	isIDRemoved func(id uint64) bool
	raft.Node
	raftStorage *raft.MemoryStorage
	storage     serverstorage.Storage
	heartbeat   time.Duration // for logging
	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	transport rafthttp.Transporter

	// localID is this node's raft ID. Used by the metronome scheme to
	// decide whether this node is in the persist-set for a given entry.
	// Zero when metronomeScheme is nil.
	localID uint64
	// metronomeScheme, if non-nil, filters which entries and snapshots
	// this node WAL-persists. HardState is always persisted regardless
	// of the scheme. The leader always persists everything.
	//
	// The pointer is stored atomically (via metronomeSchemeLive on the
	// parent raftNode) so that it can be swapped on membership changes
	// without a lock on the Ready hot path. This config field holds
	// the initial value set at bootstrap; runtime lookups go through
	// raftNode.currentScheme().
	metronomeScheme *metronome.Scheme

	// Work-stealing (Metronome §4.2). A follower tracks entries it
	// chose not to persist, alongside a timer reset on every
	// commit-index advance. If the commit stalls long enough while
	// skipped entries exist, we "steal" logging work from stragglers
	// by fsyncing the buffered entries ourselves and temporarily
	// logging every entry (not just our persist-set slot). This
	// keeps progress going when a peer in the K-of-N persist-set is
	// slow, at the cost of a small amount of extra logging on this
	// node.
	//
	// All fields below are guarded by wsMu and read/written only on
	// the raft Ready loop, so contention is minimal.
	wsMu          sync.Mutex
	wsSkipped     []uint64  // indices we did not persist (FIFO; kept sorted)
	wsLastCommit  uint64    // last seen committed index (for advance detection)
	wsLastAdvance time.Time // wall time of last commit-index advance
	wsActiveUntil time.Time // non-zero until this time => persist everything
	wsTimeout     time.Duration
	wsDuration    time.Duration
}

func newRaftNode(cfg raftNodeConfig) *raftNode {
	var lg raft.Logger
	if cfg.lg != nil {
		lg = NewRaftLoggerZap(cfg.lg)
	} else {
		lcfg := logutil.DefaultZapLoggerConfig
		var err error
		lg, err = NewRaftLogger(&lcfg)
		if err != nil {
			log.Fatalf("cannot create raft logger %v", err)
		}
	}
	raft.SetLogger(lg)
	r := &raftNode{
		lg:             cfg.lg,
		tickMu:         new(sync.RWMutex),
		raftNodeConfig: cfg,
		latestTickTs:   time.Now(),
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.heartbeat),
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan toApply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if r.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat)
	}
	if cfg.metronomeScheme != nil {
		r.metronomeSchemeLive.Store(cfg.metronomeScheme)
	}
	return r
}

// currentScheme returns the active metronome scheme, or nil if
// metronome mode is disabled. Safe to call concurrently; the pointer
// is swapped atomically on membership changes.
func (r *raftNode) currentScheme() *metronome.Scheme {
	return r.metronomeSchemeLive.Load()
}

// UpdateMetronomeScheme replaces the active scheme. Called from the
// apply loop after a ConfChange commits, so the scheme reflects the
// new membership. Passing nil is a no-op (keeps the existing scheme);
// the scheme cannot be disabled at runtime.
func (r *raftNode) UpdateMetronomeScheme(s *metronome.Scheme) {
	if s == nil {
		return
	}
	r.metronomeSchemeLive.Store(s)
	r.lg.Info("metronome scheme updated",
		zap.Int("cluster-size", s.NumNodes()),
		zap.Int("quorum-size", s.QuorumSize()),
	)
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.latestTickTs = time.Now()
	r.tickMu.Unlock()
}

func (r *raftNode) getLatestTickTs() time.Time {
	r.tickMu.RLock()
	defer r.tickMu.RUnlock()
	return r.latestTickTs
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (r *raftNode) start(rh *raftReadyHandler) {
	internalTimeout := time.Second

	go func() {
		defer r.onStop()
		islead := false

		for {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Ready():
				if rd.SoftState != nil {
					newLeader := rd.SoftState.Lead != raft.None && rh.getLead() != rd.SoftState.Lead
					if newLeader {
						leaderChanges.Inc()
					}

					if rd.SoftState.Lead == raft.None {
						hasLeader.Set(0)
					} else {
						hasLeader.Set(1)
					}

					rh.updateLead(rd.SoftState.Lead)
					islead = rd.RaftState == raft.StateLeader
					if islead {
						isLeader.Set(1)
					} else {
						isLeader.Set(0)
					}
					rh.updateLeadership(newLeader)
					r.td.Reset()
				}

				if len(rd.ReadStates) != 0 {
					select {
					case r.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
					case <-time.After(internalTimeout):
						r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
					case <-r.stopped:
						return
					}
				}

				notifyc := make(chan struct{}, 1)
				raftAdvancedC := make(chan struct{}, 1)
				ap := toApply{
					entries:       rd.CommittedEntries,
					snapshot:      rd.Snapshot,
					notifyc:       notifyc,
					raftAdvancedC: raftAdvancedC,
				}

				updateCommittedIndex(&ap, rh)

				select {
				case r.applyc <- ap:
				case <-r.stopped:
					return
				}

				// the leader can write to its disk in parallel with replicating to the followers and then
				// writing to their disks.
				// For more details, check raft thesis 10.2.1
				if islead {
					// gofail: var raftBeforeLeaderSend struct{}
					r.transport.Send(r.processMessages(rd.Messages))
				}

				// Must save the snapshot file and WAL snapshot entry before saving any other entries or hardstate to
				// ensure that recovery after a snapshot restore is possible.
				if !raft.IsEmptySnap(rd.Snapshot) {
					if r.shouldPersistSnapshot(islead, rd.Snapshot.Metadata.Index) {
						// gofail: var raftBeforeSaveSnap struct{}
						if err := r.storage.SaveSnap(rd.Snapshot); err != nil {
							r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
						}
						// gofail: var raftAfterSaveSnap struct{}
					}
				}

				// Under metronome, filter entries so only nodes in the
				// persist-set for each entry index WAL-write it. The
				// leader always persists everything for simpler recovery.
				// HardState is always passed through unchanged.
				entsToSave := rd.Entries
				if r.metronomeScheme != nil && !islead {
					entsToSave = r.filterMetronomeEntries(rd.Entries)
					r.recordReadySkipped(rd.HardState.Commit, rd.Entries, entsToSave)
				}
				// gofail: var raftBeforeSave struct{}
				if err := r.storage.Save(rd.HardState, entsToSave); err != nil {
					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
				}
				if !raft.IsEmptyHardState(rd.HardState) {
					proposalsCommitted.Set(float64(rd.HardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if !raft.IsEmptySnap(rd.Snapshot) {
					// Force WAL to fsync its hard state before Release() releases
					// old data from the WAL. Otherwise could get an error like:
					// panic: tocommit(107) is out of range [lastIndex(84)]. Was the raft log corrupted, truncated, or lost?
					// See https://github.com/etcd-io/etcd/issues/10219 for more details.
					if err := r.storage.Sync(); err != nil {
						r.lg.Fatal("failed to sync Raft snapshot", zap.Error(err))
					}

					// etcdserver now claim the snapshot has been persisted onto the disk
					notifyc <- struct{}{}

					// gofail: var raftBeforeApplySnap struct{}
					r.raftStorage.ApplySnapshot(rd.Snapshot)
					r.lg.Info("applied incoming Raft snapshot", zap.Uint64("snapshot-index", rd.Snapshot.Metadata.Index))
					// gofail: var raftAfterApplySnap struct{}

					if err := r.storage.Release(rd.Snapshot); err != nil {
						r.lg.Fatal("failed to release Raft wal", zap.Error(err))
					}
					// gofail: var raftAfterWALRelease struct{}
				}

				r.raftStorage.Append(rd.Entries)

				confChanged := false
				for _, ent := range rd.CommittedEntries {
					if ent.Type == raftpb.EntryConfChange {
						confChanged = true
						break
					}
				}

				if !islead {
					// finish processing incoming messages before we signal notifyc chan
					msgs := r.processMessages(rd.Messages)

					// now unblocks 'applyAll' that waits on Raft log disk writes before triggering snapshots
					notifyc <- struct{}{}

					// Candidate or follower needs to wait for all pending configuration
					// changes to be applied before sending messages.
					// Otherwise we might incorrectly count votes (e.g. votes from removed members).
					// Also slow machine's follower raft-layer could proceed to become the leader
					// on its own single-node cluster, before toApply-layer applies the config change.
					// We simply wait for ALL pending entries to be applied for now.
					// We might improve this later on if it causes unnecessary long blocking issues.

					if confChanged {
						// blocks until 'applyAll' calls 'applyWait.Trigger'
						// to be in sync with scheduled config-change job
						// (assume notifyc has cap of 1)
						select {
						case notifyc <- struct{}{}:
						case <-r.stopped:
							return
						}
					}

					// gofail: var raftBeforeFollowerSend struct{}
					r.transport.Send(msgs)
				} else {
					// leader already processed 'MsgSnap' and signaled
					notifyc <- struct{}{}
				}

				// gofail: var raftBeforeAdvance struct{}
				r.Advance()

				if confChanged {
					// notify etcdserver that raft has already been notified or advanced.
					raftAdvancedC <- struct{}{}
				}

				// Metronome §4.2: cheap stall-check after each Ready.
				// No-op unless metronome is enabled AND a follower saw
				// committed-index stall while sitting on skipped entries.
				if r.metronomeScheme != nil && !islead {
					r.maybeTriggerWorkSteal()
				}
			case <-r.stopped:
				return
			}
		}
	}()
}

func updateCommittedIndex(ap *toApply, rh *raftReadyHandler) {
	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
}

func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.isIDRemoved(ms[i].To) {
			ms[i].To = 0
			continue
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				r.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.String("to", fmt.Sprintf("%x", ms[i].To)),
					zap.Duration("heartbeat-interval", r.heartbeat),
					zap.Duration("expected-duration", 2*r.heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

// shouldPersistSnapshot decides whether this node should WAL-persist
// the raft snapshot at the given index. Leaders always persist. Under
// metronome, followers persist only if they are in the snapshot's
// persist-set; when the scheme is disabled, all nodes persist.
func (r *raftNode) shouldPersistSnapshot(islead bool, snapshotIndex uint64) bool {
	if r.metronomeScheme == nil || islead {
		return true
	}
	scheme := r.currentScheme()
	if scheme == nil {
		return true
	}
	return scheme.ShouldPersist(r.localID, snapshotIndex)
}

// filterMetronomeEntries returns the subset of entries that this
// follower should WAL-persist under the active metronome scheme.
// Configuration-change entries (EntryConfChange, EntryConfChangeV2) are
// always kept: membership transitions must be durable on every node so
// the scheme can be reconstructed post-restart. When the node is in
// work-stealing mode (after recent straggler-driven timeout), ALL
// entries are kept regardless of the scheme — strictly stronger than
// the scheme's K-of-N guarantee, so safety is preserved.
func (r *raftNode) filterMetronomeEntries(ents []raftpb.Entry) []raftpb.Entry {
	if len(ents) == 0 {
		return ents
	}
	if r.inWorkStealMode() {
		return ents
	}
	scheme := r.currentScheme()
	if scheme == nil {
		return ents // scheme not set yet; fall back to persist-all
	}
	out := ents[:0:0] // don't mutate caller's slice
	for i := range ents {
		e := &ents[i]
		keep := e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 ||
			scheme.ShouldPersist(r.localID, e.Index)
		if keep {
			out = append(out, *e)
		}
	}
	return out
}

// ---- Work stealing (Metronome §4.2) ---------------------------------

// inWorkStealMode returns true if this node is currently in the
// "log everything" window triggered by a recent straggler timeout.
func (r *raftNode) inWorkStealMode() bool {
	if r.metronomeScheme == nil {
		return false
	}
	r.wsMu.Lock()
	defer r.wsMu.Unlock()
	return !r.wsActiveUntil.IsZero() && time.Now().Before(r.wsActiveUntil)
}

// recordReadySkipped bookkeeps which entry indices from `allEnts` were
// filtered out of `kept` (i.e., NOT fsynced this Ready), and updates
// the commit-advance timestamp when HardState.Commit moves forward.
// Safe to call unconditionally; no-op when metronome is disabled.
func (r *raftNode) recordReadySkipped(hsCommit uint64, allEnts, kept []raftpb.Entry) {
	if r.metronomeScheme == nil {
		return
	}
	r.wsMu.Lock()
	defer r.wsMu.Unlock()

	// 1. Reset the stall timer on commit advance. Also drop skipped
	//    indices that are now committed — those entries have been
	//    committed despite us not logging them, so no straggler is
	//    hurting us on them.
	if hsCommit > r.wsLastCommit {
		r.wsLastCommit = hsCommit
		r.wsLastAdvance = time.Now()
		if len(r.wsSkipped) > 0 {
			i := 0
			for i < len(r.wsSkipped) && r.wsSkipped[i] <= hsCommit {
				i++
			}
			r.wsSkipped = r.wsSkipped[i:]
		}
	}

	// 2. In work-stealing mode, everything was persisted — no bookkeeping.
	if !r.wsActiveUntil.IsZero() && time.Now().Before(r.wsActiveUntil) {
		return
	}

	// 3. Record skipped indices (appended sorted by index; allEnts is
	//    already index-sorted, and kept is a filtered subsequence).
	if len(allEnts) == len(kept) {
		return // nothing filtered out
	}
	wasEmpty := len(r.wsSkipped) == 0
	keptByIdx := make(map[uint64]struct{}, len(kept))
	for i := range kept {
		keptByIdx[kept[i].Index] = struct{}{}
	}
	for i := range allEnts {
		e := &allEnts[i]
		if _, ok := keptByIdx[e.Index]; ok {
			continue
		}
		if e.Index <= r.wsLastCommit {
			continue // already committed; ignore
		}
		r.wsSkipped = append(r.wsSkipped, e.Index)
	}
	// 4. Arm the stall timer the moment the buffer transitions 0→N
	//    (paper §4.2: "the timer is started when an entry is added
	//    to the buffer"). Without this, the timer sometimes runs
	//    stale from an earlier Ready and fires spuriously at startup.
	if wasEmpty && len(r.wsSkipped) > 0 {
		r.wsLastAdvance = time.Now()
	}
}

// maybeTriggerWorkSteal checks the stall condition and, if met,
// flushes the buffered skipped entries to WAL and enters the
// "log everything" window for wsDuration. Called from the Ready
// loop after each iteration — cheap in the common case (O(1) time
// check; no syscalls unless we actually steal).
func (r *raftNode) maybeTriggerWorkSteal() {
	if r.metronomeScheme == nil {
		return
	}
	r.wsMu.Lock()
	// Already stealing? Exit once the window is up.
	if !r.wsActiveUntil.IsZero() {
		if time.Now().Before(r.wsActiveUntil) {
			r.wsMu.Unlock()
			return
		}
		// Window elapsed — return to normal filtering.
		r.wsActiveUntil = time.Time{}
	}
	// No skipped entries → nothing to steal.
	if len(r.wsSkipped) == 0 {
		r.wsMu.Unlock()
		return
	}
	// Don't fire until we've seen at least one real commit advance.
	// Otherwise the pre-election / initial-replication startup window
	// counts against the timeout, and every follower would immediately
	// enter "log everything" mode the first time we restart, defeating
	// metronome's whole point.
	if r.wsLastCommit == 0 {
		r.wsMu.Unlock()
		return
	}
	// Has the commit stalled long enough?
	if time.Since(r.wsLastAdvance) < r.wsTimeout {
		r.wsMu.Unlock()
		return
	}

	// Prepare the steal: collect the indices to flush and arm the
	// work-stealing window before releasing the lock, so other
	// Readies see us as "in mode" and won't add more to wsSkipped.
	toFlush := make([]uint64, len(r.wsSkipped))
	copy(toFlush, r.wsSkipped)
	r.wsSkipped = r.wsSkipped[:0]
	jitter := time.Duration(int64(r.wsDuration) / 8) // ±12.5% jitter
	r.wsActiveUntil = time.Now().Add(r.wsDuration + jitterDuration(jitter))
	r.wsMu.Unlock()

	// Pull the entries from the in-memory raft log and fsync them.
	ents := r.collectInMemoryEntries(toFlush)
	if len(ents) > 0 {
		if err := r.storage.Save(raftpb.HardState{}, ents); err != nil {
			r.lg.Warn("metronome work-steal: Save failed", zap.Error(err))
		}
	}
	metronomeWorkStealsTriggered.Inc()
	metronomeWorkStealEntries.Add(float64(len(ents)))
	r.lg.Info("metronome work-steal fired",
		zap.Int("skipped-buffered", len(toFlush)),
		zap.Int("entries-flushed", len(ents)),
		zap.Duration("window", r.wsDuration),
	)
}

// collectInMemoryEntries returns the raft entries for the given
// indices by reading from raftStorage (in-memory). Indices that are
// below the compaction floor or above the last index are silently
// skipped; the caller treats this as "nothing to do."
func (r *raftNode) collectInMemoryEntries(indices []uint64) []raftpb.Entry {
	if len(indices) == 0 {
		return nil
	}
	first, err := r.raftStorage.FirstIndex()
	if err != nil {
		return nil
	}
	last, err := r.raftStorage.LastIndex()
	if err != nil || last == 0 {
		return nil
	}
	// Group into contiguous runs to minimize Entries() calls.
	out := make([]raftpb.Entry, 0, len(indices))
	i := 0
	for i < len(indices) {
		j := i + 1
		for j < len(indices) && indices[j] == indices[j-1]+1 {
			j++
		}
		lo := indices[i]
		hi := indices[j-1] + 1
		if lo < first {
			lo = first
		}
		if hi > last+1 {
			hi = last + 1
		}
		if lo < hi {
			ents, e := r.raftStorage.Entries(lo, hi, ^uint64(0))
			if e == nil {
				wanted := make(map[uint64]struct{}, j-i)
				for k := i; k < j; k++ {
					wanted[indices[k]] = struct{}{}
				}
				for k := range ents {
					if _, ok := wanted[ents[k].Index]; ok {
						out = append(out, ents[k])
					}
				}
			}
		}
		i = j
	}
	return out
}

// jitterDuration returns a deterministic-enough pseudo-random offset
// in [-max, max]. We use time.Now() as the source so two concurrent
// recoverers don't trigger work-stealing synchronously.
func jitterDuration(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	// Tiny xorshift on nanosecond of now; acceptable for jitter.
	n := uint64(time.Now().UnixNano())
	n ^= n >> 16
	n *= 0x9E3779B97F4A7C15
	n ^= n >> 33
	// Map to [-max, max].
	return time.Duration(int64(n&0xFFFF)%int64(2*max)) - max
}

func (r *raftNode) apply() chan toApply {
	return r.applyc
}

func (r *raftNode) stop() {
	select {
	case r.stopped <- struct{}{}:
		// Not already stopped, so trigger it
	case <-r.done:
		// Has already been stopped - no need to do anything
		return
	}
	// Block until the stop has been acknowledged by start()
	<-r.done
}

func (r *raftNode) onStop() {
	r.Stop()
	r.ticker.Stop()
	r.transport.Stop()
	if err := r.storage.Close(); err != nil {
		r.lg.Panic("failed to close Raft storage", zap.Error(err))
	}
	close(r.done)
}

// for testing
func (r *raftNode) pauseSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Pause()
}

func (r *raftNode) resumeSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Resume()
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}

func (r *raftNode) ReadState() <-chan raft.ReadState {
	return r.readStateC
}
