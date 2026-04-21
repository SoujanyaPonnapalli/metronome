// Small debug tool: read a WAL directory using ReadAllSparse and
// print how many entries each index modulo N has.
package main

import (
	"flag"
	"fmt"
	"os"

	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
)

func main() {
	dir := flag.String("wal-dir", "", "path to WAL directory (e.g. .../member/wal)")
	mod := flag.Int("mod", 3, "bucket size (cluster size) for index%mod histogram")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "--wal-dir required")
		os.Exit(1)
	}
	w, err := wal.Open(zap.NewNop(), *dir, walpb.Snapshot{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer w.Close()

	_, st, ents, err := w.ReadAllSparse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ReadAllSparse: %v\n", err)
		// continue: we may still have partial output
	}

	buckets := make([]int, *mod)
	var minIdx, maxIdx uint64
	for i, e := range ents {
		buckets[e.Index%uint64(*mod)]++
		if i == 0 {
			minIdx = e.Index
		}
		maxIdx = e.Index
	}
	fmt.Printf("wal-dir:    %s\n", *dir)
	fmt.Printf("hardstate:  term=%d vote=%d commit=%d\n", st.Term, st.Vote, st.Commit)
	fmt.Printf("entries:    count=%d min=%d max=%d span=%d\n", len(ents), minIdx, maxIdx, maxIdx-minIdx+1)
	fmt.Printf("bucket by index %% %d:\n", *mod)
	for i, n := range buckets {
		fmt.Printf("  bucket %d: %d entries\n", i, n)
	}
}
