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

// Package metronome implements a deterministic log/snapshot shuffling scheme
// that reduces redundant disk writes across a Raft cluster.
//
// Under standard Raft every node WAL-persists every log entry, so every
// committed entry is on disk on all N replicas. Metronome decouples
// durability from agreement: only a rotating K-sized persist-set
// (K >= f+1) actually WAL-writes each entry; the remaining N-K nodes keep
// the entry only in their in-memory raft.MemoryStorage and ACK from
// memory.
//
// Safety: because every persist-set is a majority (K >= f+1) and any
// majority intersects every other majority, each committed entry is
// durably recoverable from any future majority. Election safety is
// preserved because HardState (term, vote, commit) is still persisted on
// every node on every ready event.
//
// Load balancing: the persist-set rotates by one position per entry
// index, so consecutive entries share (K-1) persisters. Over the long
// run each node persists K/N of all entries.
//
// This package is pure computation — it has no side effects and does not
// import anything from storage/WAL. Callers wire ShouldPersist() into
// their storage save path.
package metronome

import (
	"fmt"
	"sort"
)

// Scheme is a deterministic round-robin persist-set picker. It is
// constructed from the current cluster membership and quorum size and is
// immutable thereafter. On membership change, callers construct a new
// Scheme from the updated membership.
type Scheme struct {
	// nodeIDs is the membership sorted ascending. Using a sorted order
	// (rather than insertion order) makes the scheme identical across
	// nodes regardless of how the membership list was built.
	nodeIDs []uint64

	// quorumSize is K: the number of nodes that persist each entry.
	// Invariant: f+1 <= quorumSize <= len(nodeIDs).
	quorumSize int
}

// DefaultQuorumSize returns f+1 = ceil((numNodes+1)/2), the smallest
// quorum size that tolerates f = floor((numNodes-1)/2) failures.
func DefaultQuorumSize(numNodes int) int {
	if numNodes < 1 {
		return 1
	}
	return numNodes/2 + 1
}

// NewScheme validates the arguments and returns a Scheme.
// quorumSize of 0 selects the default (f+1).
func NewScheme(nodeIDs []uint64, quorumSize int) (*Scheme, error) {
	if len(nodeIDs) < 1 {
		return nil, fmt.Errorf("metronome: nodeIDs must be non-empty")
	}
	if quorumSize == 0 {
		quorumSize = DefaultQuorumSize(len(nodeIDs))
	}
	minQ := DefaultQuorumSize(len(nodeIDs))
	if quorumSize < minQ {
		return nil, fmt.Errorf("metronome: quorumSize %d < f+1 (%d)", quorumSize, minQ)
	}
	if quorumSize > len(nodeIDs) {
		return nil, fmt.Errorf("metronome: quorumSize %d > N (%d)", quorumSize, len(nodeIDs))
	}
	sorted := make([]uint64, len(nodeIDs))
	copy(sorted, nodeIDs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[i-1] {
			return nil, fmt.Errorf("metronome: duplicate nodeID %d", sorted[i])
		}
	}
	return &Scheme{nodeIDs: sorted, quorumSize: quorumSize}, nil
}

// NumNodes returns N.
func (s *Scheme) NumNodes() int { return len(s.nodeIDs) }

// QuorumSize returns K.
func (s *Scheme) QuorumSize() int { return s.quorumSize }

// NodeIDs returns a copy of the sorted nodeIDs.
func (s *Scheme) NodeIDs() []uint64 {
	out := make([]uint64, len(s.nodeIDs))
	copy(out, s.nodeIDs)
	return out
}

// PersistSet returns the K nodeIDs that should persist the entry at
// `index`. The caller must not mutate the returned slice.
func (s *Scheme) PersistSet(index uint64) []uint64 {
	n := len(s.nodeIDs)
	start := int(index % uint64(n))
	out := make([]uint64, s.quorumSize)
	for i := 0; i < s.quorumSize; i++ {
		out[i] = s.nodeIDs[(start+i)%n]
	}
	return out
}

// ShouldPersist reports whether nodeID should WAL-persist the entry at
// `index` under this scheme. It returns false when nodeID is not a
// member of the scheme.
func (s *Scheme) ShouldPersist(nodeID uint64, index uint64) bool {
	n := len(s.nodeIDs)
	start := int(index % uint64(n))
	for i := 0; i < s.quorumSize; i++ {
		if s.nodeIDs[(start+i)%n] == nodeID {
			return true
		}
	}
	return false
}
