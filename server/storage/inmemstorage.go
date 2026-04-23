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

package storage

import (
	"github.com/coreos/go-semver/semver"
	"go.etcd.io/raft/v3/raftpb"
)

// InMemStorage implements Storage without any disk I/O.
// It is used when ExperimentalInMemOnly is set. Log entries live entirely
// in raft.MemoryStorage (populated by raftNode.start at raft.go:285);
// this implementation is intentionally a no-op for all persistence methods.
type InMemStorage struct{}

func NewInMemStorage() *InMemStorage { return &InMemStorage{} }

// Save is a no-op. Entries are already held in raft.MemoryStorage.
func (s *InMemStorage) Save(_ raftpb.HardState, _ []raftpb.Entry) error { return nil }

// SaveSnap is a no-op. Snapshots are managed by InMemSnapshotter.
func (s *InMemStorage) SaveSnap(_ raftpb.Snapshot) error { return nil }

func (s *InMemStorage) Release(_ raftpb.Snapshot) error { return nil }

func (s *InMemStorage) Sync() error { return nil }

func (s *InMemStorage) Close() error { return nil }

// MinimalEtcdVersion returns nil because there are no WAL entries to inspect.
func (s *InMemStorage) MinimalEtcdVersion() *semver.Version { return nil }
