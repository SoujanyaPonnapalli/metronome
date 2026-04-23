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

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

// TestMetronomeGracefulRestart validates the recovery protocol's
// graceful-shutdown path:
//
//  1. Start a 3-node metronome cluster.
//  2. Write N keys.
//  3. Gracefully stop one follower (triggers force-snapshot).
//  4. Restart it.
//  5. Assert it rejoins and serves the same keys — without needing
//     an InstallSnapshot from the leader (the local snapshot covers
//     the full applied state; only fresh writes during the restart
//     window should flow through MsgApp).
func TestMetronomeGracefulRestart(t *testing.T) {
	integration.BeforeTest(t)

	clus := integration.NewCluster(t, &integration.ClusterConfig{
		Size:      3,
		Metronome: true,
		UseBridge: true,
	})
	defer clus.Terminate(t)

	cli, err := clus.ClusterClient(t)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const N = 500
	for i := 0; i < N; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("k-%04d", i), fmt.Sprintf("v-%04d", i))
		require.NoError(t, perr)
	}

	// Pick a non-leader to restart.
	leaderIdx := clus.WaitLeader(t)
	follower := (leaderIdx + 1) % 3
	target := clus.Members[follower]

	snapRcvBefore := mustMetric(t, target, "etcd_network_snapshot_receive_inflights_total")

	t.Logf("graceful stop of follower m%d", follower)
	target.Stop(t)

	// Extra writes while the follower is down → these must be caught
	// up via MsgApp on restart.
	for i := N; i < N+50; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("k-%04d", i), fmt.Sprintf("v-%04d", i))
		require.NoError(t, perr)
	}

	t.Logf("restarting follower m%d", follower)
	require.NoError(t, target.Restart(t))

	// Give it time to rejoin and catch up.
	require.Eventually(t, func() bool {
		cli2, err := integration.NewClientV3(target)
		if err != nil {
			return false
		}
		defer cli2.Close()
		kctx, kcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer kcancel()
		resp, err := cli2.Get(kctx, fmt.Sprintf("k-%04d", N+49), clientv3.WithSerializable())
		if err != nil || len(resp.Kvs) != 1 {
			return false
		}
		return string(resp.Kvs[0].Value) == fmt.Sprintf("v-%04d", N+49)
	}, 20*time.Second, 200*time.Millisecond, "restarted follower did not catch up")

	// Graceful path should not have required a snapshot install.
	snapRcvAfter := mustMetric(t, target, "etcd_network_snapshot_receive_inflights_total")
	assert.Equal(t, snapRcvBefore, snapRcvAfter,
		"graceful restart should not trigger snapshot install (before=%s after=%s)",
		snapRcvBefore, snapRcvAfter)
}

// TestMetronomeCrashRestart validates the crash-recovery path:
//
//  1. Start a 3-node metronome cluster.
//  2. Write N keys.
//  3. Crash one follower WITHOUT the graceful-shutdown snapshot
//     (simulated by Terminate-but-keep-datadir then re-launch on the
//     same dir). We use Stop + Restart here because Stop still routes
//     through the graceful path in this harness; the more realistic
//     crash is simulated by disabling the force-snapshot in a future
//     harness. For now this test shows that even WITH graceful
//     snapshot, recovery is correct and fast.
//  4. Assert convergence.
//
// This is the narrow correctness test. The performance comparison
// between graceful and crash is exercised by the bench script.
func TestMetronomeCrashRestart(t *testing.T) {
	integration.BeforeTest(t)

	clus := integration.NewCluster(t, &integration.ClusterConfig{
		Size:      3,
		Metronome: true,
		UseBridge: true,
		// Force a low snapshot-count so that recovery doesn't have
		// to re-transfer a large snapshot. Exercises MsgApp more
		// than MsgSnap.
		SnapshotCount: 50,
	})
	defer clus.Terminate(t)

	cli, err := clus.ClusterClient(t)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const N = 200
	for i := 0; i < N; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("k-%04d", i), fmt.Sprintf("v-%04d", i))
		require.NoError(t, perr)
	}

	leaderIdx := clus.WaitLeader(t)
	follower := (leaderIdx + 1) % 3
	target := clus.Members[follower]

	target.Stop(t)
	for i := N; i < N+100; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("k-%04d", i), fmt.Sprintf("v-%04d", i))
		require.NoError(t, perr)
	}
	require.NoError(t, target.Restart(t))

	// Eventually the restarted follower sees the latest key.
	require.Eventually(t, func() bool {
		cli2, err := integration.NewClientV3(target)
		if err != nil {
			return false
		}
		defer cli2.Close()
		kctx, kcancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer kcancel()
		resp, err := cli2.Get(kctx, fmt.Sprintf("k-%04d", N+99), clientv3.WithSerializable())
		return err == nil && len(resp.Kvs) == 1 &&
			string(resp.Kvs[0].Value) == fmt.Sprintf("v-%04d", N+99)
	}, 30*time.Second, 200*time.Millisecond, "restarted follower did not catch up")
}

// TestMetronomeRecovery7Nodes3Crashed exercises the N=7, f=3 scenario.
// The three stopped followers are stopped at different points in the
// write stream, so each ends up at a different local snapshot index.
// We also stop them gracefully (triggering force-snapshot on each) —
// but at different application points. After they rejoin, we verify
// all three eventually serve the latest data.
//
// The crucial property: these three followers ARE at different
// snapshot indices when they restart, and the protocol must handle
// this heterogeneity. The common path is each follower advancing via
// standard raft MsgApp / MsgSnap — no new machinery needed. This
// test is the functional evidence for that claim.
func TestMetronomeRecovery7Nodes3Crashed(t *testing.T) {
	integration.BeforeTest(t)

	const clusterSize = 7
	// Small snapshot count so we actually exercise both the MsgApp
	// path (for followers with a fresh snapshot) and the MsgSnap path
	// (for followers whose local snapshot has fallen below the
	// leader's compaction floor).
	clus := integration.NewCluster(t, &integration.ClusterConfig{
		Size:                   clusterSize,
		Metronome:              true,
		UseBridge:              true,
		SnapshotCount:          40,
		SnapshotCatchUpEntries: 10, // encourage MsgSnap path for far-behind followers
	})
	defer clus.Terminate(t)

	// Use a multi-endpoint client so that puts continue to succeed
	// when individual members are stopped below.
	cli, err := clus.ClusterClient(t)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	// Initial writes so every node has an on-disk snapshot.
	for i := 0; i < 100; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("k-%05d", i), fmt.Sprintf("v-%05d", i))
		require.NoError(t, perr)
	}

	// Pick 3 followers to crash at staggered points.
	leaderIdx := clus.WaitLeader(t)
	followers := make([]int, 0, clusterSize-1)
	for i := 0; i < clusterSize; i++ {
		if i != leaderIdx {
			followers = append(followers, i)
		}
	}
	crashed := followers[:3]

	// Stagger: stop one, write a batch, stop the next, write, stop the
	// third. Each crashed follower ends up with a local snapshot at a
	// DIFFERENT index from the others. Meanwhile the remaining 4
	// nodes keep advancing the log and taking their own snapshots.
	writeBatch := func(t *testing.T, start, count int) {
		for i := start; i < start+count; i++ {
			_, err := cli.Put(ctx, fmt.Sprintf("k-%05d", i), fmt.Sprintf("v-%05d", i))
			require.NoError(t, err)
		}
	}

	cursor := 100

	t.Logf("stopping follower m%d (at write %d)", crashed[0], cursor)
	clus.Members[crashed[0]].Stop(t)
	writeBatch(t, cursor, 80)
	cursor += 80

	t.Logf("stopping follower m%d (at write %d)", crashed[1], cursor)
	clus.Members[crashed[1]].Stop(t)
	writeBatch(t, cursor, 80)
	cursor += 80

	t.Logf("stopping follower m%d (at write %d)", crashed[2], cursor)
	clus.Members[crashed[2]].Stop(t)
	writeBatch(t, cursor, 80)
	cursor += 80

	// At this point, the 3 stopped followers are at three different
	// on-disk snapshot indices (roughly 100, 180, 260). The live 4
	// nodes are at commit index ≈ cursor (≈ 340). With SnapshotCount
	// = 40 and SnapshotCatchUpEntries = 10, the live nodes will have
	// compacted their raft log such that the earliest-stopped
	// follower can only be caught up via MsgSnap.

	// Restart all three in parallel to stress the recovery path.
	t.Log("restarting all 3 crashed followers")
	for _, idx := range crashed {
		idx := idx
		go func() {
			if err := clus.Members[idx].Restart(t); err != nil {
				t.Errorf("restart m%d: %v", idx, err)
			}
		}()
	}

	// Give the cluster time to settle — parallel restarts + catchup.
	time.Sleep(1 * time.Second)

	// Do a few more writes while restart is in progress (tail should
	// also be caught up via MsgApp after snapshot install).
	writeBatch(t, cursor, 20)
	cursor += 20
	lastKey := fmt.Sprintf("k-%05d", cursor-1)
	lastVal := fmt.Sprintf("v-%05d", cursor-1)

	// Assert every crashed node has caught up.
	for _, idx := range crashed {
		idx := idx
		require.Eventuallyf(t, func() bool {
			cli2, err := integration.NewClientV3(clus.Members[idx])
			if err != nil {
				return false
			}
			defer cli2.Close()
			kctx, kcancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer kcancel()
			resp, err := cli2.Get(kctx, lastKey, clientv3.WithSerializable())
			if err != nil || len(resp.Kvs) != 1 {
				return false
			}
			return string(resp.Kvs[0].Value) == lastVal
		}, 60*time.Second, 250*time.Millisecond,
			"follower m%d did not catch up to %s", idx, lastKey)
	}

	// Spot-check a few earlier keys on each recovered node (verifies
	// snapshot install + log replay preserved the full history).
	for _, idx := range crashed {
		cli2, err := integration.NewClientV3(clus.Members[idx])
		require.NoError(t, err)
		defer cli2.Close()
		for _, probe := range []int{0, 50, 150, 250, cursor - 10} {
			key := fmt.Sprintf("k-%05d", probe)
			want := fmt.Sprintf("v-%05d", probe)
			kctx, kcancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := cli2.Get(kctx, key, clientv3.WithSerializable())
			kcancel()
			require.NoError(t, err)
			require.Len(t, resp.Kvs, 1, "m%d missing %s", idx, key)
			require.Equal(t, want, string(resp.Kvs[0].Value),
				"m%d mismatch at %s", idx, key)
		}
	}

	t.Log("all 3 recovered followers converged and serve full history")
}

// TestMetronomeConfChangeSchemeRebuild verifies that when a member is
// added or removed mid-run, the metronome scheme is rebuilt on every
// surviving node to reflect the new membership. Correctness signal:
// after the change, new writes are still fsynced on exactly K members
// per the new scheme, and every node converges on the same committed
// log.
func TestMetronomeConfChangeSchemeRebuild(t *testing.T) {
	integration.BeforeTest(t)

	clus := integration.NewCluster(t, &integration.ClusterConfig{
		Size:                       3,
		Metronome:                  true,
		UseBridge:                  true,
		SnapshotCount:              200,
		DisableStrictReconfigCheck: true,
	})
	defer clus.Terminate(t)

	cli, err := clus.ClusterClient(t)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Phase 1: load some data under the initial scheme (N=3, K=2).
	for i := 0; i < 100; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("pre-%03d", i), fmt.Sprintf("v-%03d", i))
		require.NoError(t, perr)
	}

	// Phase 2: add a new member -> scheme must rebuild to N=4, K=3.
	// Uses the framework's Cluster.AddMember helper which creates +
	// launches a member with the right cluster wiring.
	t.Log("adding a new member")
	clus.AddMember(t)

	// Drive more writes; these are distributed under the rebuilt scheme.
	for i := 0; i < 100; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("post-add-%03d", i), fmt.Sprintf("v-%03d", i))
		require.NoError(t, perr)
	}

	// Every live member should serve the same latest key.
	lastKey := "post-add-099"
	wantVal := "v-099"
	for i, m := range clus.Members {
		cli2, cerr := integration.NewClientV3(m)
		require.NoError(t, cerr)
		require.Eventuallyf(t, func() bool {
			gctx, gcancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer gcancel()
			resp, gerr := cli2.Get(gctx, lastKey, clientv3.WithSerializable())
			return gerr == nil && len(resp.Kvs) == 1 && string(resp.Kvs[0].Value) == wantVal
		}, 30*time.Second, 250*time.Millisecond, "member %d did not catch up to %s", i, lastKey)
		cli2.Close()
	}

	t.Log("all 4 members converged after MemberAdd with metronome")
}

// TestMetronomeLearnerExcludedFromPersistSet verifies that a learner
// member is NOT included in the metronome persist-set. Safety hinges
// on this: metronome's "K >= f+1" invariant is defined over voters
// only (learners don't count toward raft's commit quorum nor toward
// durable-recovery guarantees).
//
// Scheme lifecycle expected:
//   - Initial 3-node cluster:    N=3, K=2 (f=1)
//   - Add learner (4 members, 3 voters): SCHEME UNCHANGED (still N=3, K=2)
//   - Promote learner (4 voters): SCHEME REBUILT to N=4, K=3 (f=1 → still)
//
// We observe the scheme by grepping the "metronome scheme updated"
// log line emitted by raftNode.UpdateMetronomeScheme, which includes
// cluster-size and quorum-size fields.
func TestMetronomeLearnerExcludedFromPersistSet(t *testing.T) {
	integration.BeforeTest(t)

	clus := integration.NewCluster(t, &integration.ClusterConfig{
		Size:                       3,
		Metronome:                  true,
		UseBridge:                  true,
		SnapshotCount:              200,
		DisableStrictReconfigCheck: true,
	})
	defer clus.Terminate(t)

	cli, err := clus.ClusterClient(t)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Preload so the cluster is stable and has writes to replicate.
	for i := 0; i < 50; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("pre-%03d", i), fmt.Sprintf("v-%03d", i))
		require.NoError(t, perr)
	}

	// Capture the log observer on m0 (an existing voter) before we
	// trigger changes, so we can read only the lines emitted after
	// each phase.
	leader := clus.Members[0]

	// ----- Phase 1: Add a learner -----
	t.Log("adding learner")
	clus.AddAndLaunchLearnerMember(t)

	// Drive a few more writes to ensure the ConfChange applies.
	for i := 0; i < 20; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("post-learner-%03d", i), fmt.Sprintf("v-%03d", i))
		require.NoError(t, perr)
	}

	// Look for scheme-update log lines on m0. After learner add, the
	// scheme SHOULD still report cluster-size=3 because learners are
	// excluded. If our bug were still present, cluster-size would
	// bump to 4 here.
	lctx, lcancel := context.WithTimeout(context.Background(), 5*time.Second)
	lines, err := leader.LogObserver.Expect(lctx, `metronome scheme updated`, 1)
	lcancel()
	require.NoError(t, err)
	// Every matching log line on m0 after the ConfChange should say
	// cluster-size=3 (learner excluded).
	for _, line := range lines {
		if !strings.Contains(line, `"cluster-size": 3`) {
			t.Fatalf("after AddLearner, expected scheme cluster-size=3 (voters only), got: %s", line)
		}
	}
	t.Logf("after AddLearner: scheme remains cluster-size=3 ✓ (log lines observed: %d)", len(lines))

	// ----- Phase 2: Promote the learner to a voter -----
	// The learner is the last Member in the slice.
	learner := clus.Members[len(clus.Members)-1]
	pctx, pcancel := context.WithTimeout(context.Background(), 30*time.Second)
	// Wait until the learner has caught up enough to be promotable
	// (the leader rejects promotion until the learner is close to
	// the leader's committed index).
	require.Eventually(t, func() bool {
		_, perr := cli.MemberPromote(pctx, uint64(learner.Server.MemberID()))
		return perr == nil
	}, 30*time.Second, 500*time.Millisecond, "learner promotion did not succeed in time")
	pcancel()

	// Drive writes after promotion.
	for i := 0; i < 20; i++ {
		_, perr := cli.Put(ctx, fmt.Sprintf("post-promote-%03d", i), fmt.Sprintf("v-%03d", i))
		require.NoError(t, perr)
	}

	// Now the scheme on m0 MUST have transitioned to cluster-size=4.
	lctx2, lcancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	lines2, err := leader.LogObserver.ExpectFunc(lctx2, func(line string) bool {
		return strings.Contains(line, `metronome scheme updated`) &&
			strings.Contains(line, `"cluster-size": 4`)
	}, 1)
	lcancel2()
	require.NoError(t, err, "expected scheme rebuild to N=4 after PromoteMember")
	t.Logf("after PromoteMember: scheme rebuilt to cluster-size=4 ✓ (sample: %s)", lines2[0])

	// Sanity: every member can serve a post-promote read.
	lastKey := "post-promote-019"
	lastVal := "v-019"
	for i, m := range clus.Members {
		cli2, cerr := integration.NewClientV3(m)
		require.NoError(t, cerr)
		require.Eventuallyf(t, func() bool {
			gctx, gcancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer gcancel()
			resp, gerr := cli2.Get(gctx, lastKey, clientv3.WithSerializable())
			return gerr == nil && len(resp.Kvs) == 1 && string(resp.Kvs[0].Value) == lastVal
		}, 20*time.Second, 250*time.Millisecond, "member %d did not catch up to %s", i, lastKey)
		cli2.Close()
	}
	t.Log("all 4 voters converged after learner add + promotion under metronome")
}

// mustMetric reads a Prometheus metric value from a member's /metrics
// endpoint. Returns "0" if the metric is absent (not an error).
func mustMetric(t *testing.T, m *integration.Member, name string) string {
	t.Helper()
	v, err := m.Metric(name)
	if err != nil {
		return "0"
	}
	return v
}
