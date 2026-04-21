// Copyright 2024 The etcd Authors
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

package snap

import (
	"fmt"
	"io"
	"os"
	"sync"

	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// InMemSnapshotter implements Snapshotter without any disk I/O.
// Raft snapshots (metadata) are stored in memory. DB snapshots are written
// to a temporary OS file only when DBFilePath is called (e.g. for peer
// transfer over HTTP), and cleaned up on ReleaseSnapDBs.
type InMemSnapshotter struct {
	lg *zap.Logger

	mu    sync.RWMutex
	snaps []raftpb.Snapshot  // ordered by index, newest last
	dbs   map[uint64]*dbEntry // index → DB snapshot
}

type dbEntry struct {
	data    []byte
	tmpPath string // non-empty once materialised to a temp file
}

func NewInMem(lg *zap.Logger) *InMemSnapshotter {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &InMemSnapshotter{
		lg:  lg,
		dbs: make(map[uint64]*dbEntry),
	}
}

func (s *InMemSnapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snaps = append(s.snaps, snapshot)
	return nil
}

func (s *InMemSnapshotter) Load() (*raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	snap := s.snaps[len(s.snaps)-1]
	return &snap, nil
}

// LoadNewestAvailable returns the newest snapshot. In in-memory mode there is
// no WAL to cross-reference, so walSnaps is ignored and the newest snapshot is
// returned unconditionally.
func (s *InMemSnapshotter) LoadNewestAvailable(_ []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return s.Load()
}

// SaveDBFrom reads the incoming DB snapshot body into memory.
func (s *InMemSnapshotter) SaveDBFrom(r io.Reader, id uint64) (int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.dbs[id]; !ok {
		s.dbs[id] = &dbEntry{data: data}
	}
	s.lg.Info("saved database snapshot to memory", zap.Uint64("snapshot-index", id), zap.Int("bytes", len(data)))
	return int64(len(data)), nil
}

// DBFilePath materialises the in-memory DB snapshot to a temporary file so
// that the rafthttp snapshot sender can stream it to a peer. The temp file is
// tracked and deleted on ReleaseSnapDBs.
func (s *InMemSnapshotter) DBFilePath(id uint64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.dbs[id]
	if !ok {
		return "", ErrNoDBSnapshot
	}
	if entry.tmpPath != "" {
		// already materialised
		return entry.tmpPath, nil
	}
	f, err := os.CreateTemp("", fmt.Sprintf("%016x.snap.db", id))
	if err != nil {
		return "", err
	}
	if _, err = f.Write(entry.data); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", err
	}
	f.Close()
	entry.tmpPath = f.Name()
	return entry.tmpPath, nil
}

// ReleaseSnapDBs removes all DB snapshot entries (and any temp files) whose
// index is less than or equal to the given snapshot's index.
func (s *InMemSnapshotter) ReleaseSnapDBs(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range s.dbs {
		if id <= snap.Metadata.Index {
			if entry.tmpPath != "" {
				os.Remove(entry.tmpPath)
			}
			delete(s.dbs, id)
		}
	}
	return nil
}
