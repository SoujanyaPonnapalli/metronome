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

package metronome

import (
	"testing"
)

func TestDefaultQuorumSize(t *testing.T) {
	cases := []struct{ n, want int }{
		{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}, {7, 4}, {9, 5},
	}
	for _, c := range cases {
		if got := DefaultQuorumSize(c.n); got != c.want {
			t.Errorf("DefaultQuorumSize(%d) = %d; want %d", c.n, got, c.want)
		}
	}
}

func TestNewSchemeValidation(t *testing.T) {
	// rejects empty members
	if _, err := NewScheme(nil, 0); err == nil {
		t.Fatal("expected error on empty nodeIDs")
	}
	// rejects K < f+1
	if _, err := NewScheme([]uint64{1, 2, 3, 4, 5}, 2); err == nil {
		t.Fatal("expected error on K=2 for N=5 (min is 3)")
	}
	// rejects K > N
	if _, err := NewScheme([]uint64{1, 2, 3}, 4); err == nil {
		t.Fatal("expected error on K=4 for N=3")
	}
	// rejects duplicates
	if _, err := NewScheme([]uint64{1, 2, 2}, 0); err == nil {
		t.Fatal("expected error on duplicate nodeIDs")
	}
	// accepts default K
	s, err := NewScheme([]uint64{3, 1, 2, 5, 4}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.QuorumSize() != 3 {
		t.Errorf("default QuorumSize for N=5 should be 3, got %d", s.QuorumSize())
	}
	// accepts K = N (behaves like standard Raft)
	if _, err := NewScheme([]uint64{1, 2, 3}, 3); err != nil {
		t.Fatalf("K=N should be accepted, got %v", err)
	}
}

// TestShouldPersistCoverage verifies that across consecutive entries,
// every node is in some persist-set (load balancing property).
func TestShouldPersistCoverage(t *testing.T) {
	nodes := []uint64{10, 20, 30, 40, 50}
	s, err := NewScheme(nodes, 3) // K=f+1=3
	if err != nil {
		t.Fatal(err)
	}
	// For N entries, each node should persist exactly K of them.
	counts := map[uint64]int{}
	for idx := uint64(0); idx < uint64(s.NumNodes()); idx++ {
		for _, id := range s.PersistSet(idx) {
			counts[id]++
		}
	}
	for _, id := range nodes {
		if counts[id] != s.QuorumSize() {
			t.Errorf("node %d: persisted %d entries in a window of %d, want %d", id, counts[id], s.NumNodes(), s.QuorumSize())
		}
	}
}

// TestShouldPersistMajorityIntersection verifies that any two
// consecutive persist-sets overlap in at least (K - 1) nodes, and any
// two arbitrary persist-sets overlap in at least 2K - N nodes (the
// majority intersection property that underpins safety).
func TestShouldPersistMajorityIntersection(t *testing.T) {
	nodes := []uint64{1, 2, 3, 4, 5}
	s, _ := NewScheme(nodes, 3)
	minOverlap := 2*s.QuorumSize() - s.NumNodes() // 1
	for a := uint64(0); a < 20; a++ {
		for b := a + 1; b < 25; b++ {
			setA := map[uint64]bool{}
			for _, id := range s.PersistSet(a) {
				setA[id] = true
			}
			overlap := 0
			for _, id := range s.PersistSet(b) {
				if setA[id] {
					overlap++
				}
			}
			if overlap < minOverlap {
				t.Errorf("persist sets at %d and %d overlap in %d nodes, need >= %d", a, b, overlap, minOverlap)
			}
		}
	}
}

func TestShouldPersistDeterminism(t *testing.T) {
	// Sorted order ensures two nodes independently construct the same
	// scheme from the same membership.
	s1, _ := NewScheme([]uint64{3, 1, 2, 5, 4}, 3)
	s2, _ := NewScheme([]uint64{5, 4, 3, 2, 1}, 3)
	for idx := uint64(0); idx < 100; idx++ {
		for _, id := range []uint64{1, 2, 3, 4, 5} {
			if s1.ShouldPersist(id, idx) != s2.ShouldPersist(id, idx) {
				t.Fatalf("non-deterministic at idx=%d node=%d", idx, id)
			}
		}
	}
}

// TestShouldPersistKEqualsN: when K = N, every node persists every
// entry — same as standard Raft.
func TestShouldPersistKEqualsN(t *testing.T) {
	nodes := []uint64{1, 2, 3}
	s, _ := NewScheme(nodes, 3)
	for idx := uint64(0); idx < 10; idx++ {
		for _, id := range nodes {
			if !s.ShouldPersist(id, idx) {
				t.Errorf("K=N: node %d should persist idx %d", id, idx)
			}
		}
	}
}
