package cluster

import (
	"path/filepath"
	"reflect"
	"testing"
)

// TestConfigEntryMechanism pins Stage 2: config encode/decode round-trips; a config
// entry in the log is adopted (and a truncation reverts to what remains, else the
// base config) by deriveConfigLocked; and the raft.log frame carries the entry kind
// and the config payload across a reload.
func TestConfigEntryMechanism(t *testing.T) {
	cfg := map[NodeID]bool{0: true, 2: true, 3: true}
	if got := decodeConfig(encodeConfig(cfg)); !configEqual(got, cfg) {
		t.Fatalf("config round-trip = %v, want %v", got, cfg)
	}

	base := map[NodeID]bool{0: true, 1: true, 2: true, 3: true, 4: true}
	l := NewRaftLog()
	l.append(1, EntryData, []byte("d1"))                                               // 1: data
	l.append(1, EntryConfig, encodeConfig(map[NodeID]bool{0: true, 1: true, 2: true})) // 2: config {0,1,2}
	l.append(1, EntryData, []byte("d2"))                                               // 3: data
	n := &Node{id: 0, log: l, baseConfig: base}

	n.deriveConfigLocked()
	if !configEqual(n.config, map[NodeID]bool{0: true, 1: true, 2: true}) {
		t.Fatalf("derived config = %v, want {0,1,2}", n.config)
	}
	l.truncateFrom(2) // drop the config entry
	n.deriveConfigLocked()
	if !configEqual(n.config, base) {
		t.Fatalf("config after truncating the config entry = %v, want base %v", n.config, base)
	}

	// The raft.log frame preserves the kind and payload across a reload.
	path := filepath.Join(t.TempDir(), raftLogFileName)
	lf, _, err := openRaftLogFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := lf.append(1, EntryData, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := lf.append(2, EntryConfig, encodeConfig(map[NodeID]bool{1: true, 2: true})); err != nil {
		t.Fatal(err)
	}
	lf.close()

	_, entries, err := openRaftLogFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("reloaded %d entries, want 2", len(entries))
	}
	if entries[0].kind != EntryData || entries[1].kind != EntryConfig {
		t.Fatalf("kinds not preserved across reload: %v, %v", entries[0].kind, entries[1].kind)
	}
	if !configEqual(decodeConfig(entries[1].data), map[NodeID]bool{1: true, 2: true}) {
		t.Fatalf("config entry payload not preserved across reload")
	}
}

// TestCompactionFoldsConfig pins Stage 6a: when Raft-log compaction discards a
// prefix containing a configuration entry, that configuration is folded into the
// durable base-config file so it survives a restart (the log no longer holds the
// entry to re-derive it). readBaseConfigFile is exactly what buildNode reads at
// startup, so a correct base file here means a correct reconstruction on restart.
func TestCompactionFoldsConfig(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), raftConfigFileName)
	l := NewRaftLog()
	l.append(1, EntryData, []byte("d1"))                                               // 1
	l.append(1, EntryConfig, encodeConfig(map[NodeID]bool{0: true, 1: true, 2: true})) // 2: {0,1,2}
	l.append(1, EntryData, []byte("d2"))                                               // 3
	l.append(1, EntryConfig, encodeConfig(map[NodeID]bool{0: true, 1: true}))          // 4: {0,1}
	l.append(1, EntryData, []byte("d3"))                                               // 5

	base := map[NodeID]bool{0: true, 1: true, 2: true, 3: true}
	n := &Node{
		log:         l,
		baseConfig:  copyConfig(base),
		config:      map[NodeID]bool{0: true, 1: true},
		configPath:  configPath,
		cfg:         Config{SnapshotThreshold: 1}, // compact whenever asked
		lastApplied: 4,                            // discard through index 4 (the {0,1} entry)
	}
	n.raftMu.Lock()
	n.maybeCompactLocked()
	n.raftMu.Unlock()

	// The base-config file (and the in-memory base) now hold the config as of the
	// new base, index 4 = {0,1}; the log compacted to base 4.
	if got, ok := readBaseConfigFile(configPath); !ok || !configEqual(got, map[NodeID]bool{0: true, 1: true}) {
		t.Fatalf("base-config file after compaction = %v (ok=%v), want {0,1}", got, ok)
	}
	if !configEqual(n.baseConfig, map[NodeID]bool{0: true, 1: true}) {
		t.Fatalf("in-memory baseConfig = %v, want {0,1}", n.baseConfig)
	}
	if n.log.baseIndex != 4 {
		t.Fatalf("log baseIndex = %d, want 4", n.log.baseIndex)
	}
}

// TestCompactionNoConfigLeavesBase pins the other branch: compacting a prefix with
// no configuration entry leaves the base config untouched (and writes no file).
func TestCompactionNoConfigLeavesBase(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), raftConfigFileName)
	l := NewRaftLog()
	l.append(1, EntryData, []byte("d1")) // 1
	l.append(1, EntryData, []byte("d2")) // 2
	l.append(1, EntryData, []byte("d3")) // 3

	base := map[NodeID]bool{0: true, 1: true, 2: true}
	n := &Node{
		log:         l,
		baseConfig:  copyConfig(base),
		config:      copyConfig(base),
		configPath:  configPath,
		cfg:         Config{SnapshotThreshold: 1},
		lastApplied: 2,
	}
	n.raftMu.Lock()
	n.maybeCompactLocked()
	n.raftMu.Unlock()

	if !configEqual(n.baseConfig, base) {
		t.Fatalf("baseConfig changed without a compacted config entry: %v", n.baseConfig)
	}
	if _, ok := readBaseConfigFile(configPath); ok {
		t.Fatalf("base-config file was written despite no config entry in the compacted prefix")
	}
}

// TestConfigDrivesQuorum pins Stage 1: the quorum size and the vote-solicitation
// set derive from the config, so shrinking or growing it changes the majority — and
// a nil config falls back to the fixed peer set for bare-Node white-box tests.
func TestConfigDrivesQuorum(t *testing.T) {
	five := &Node{id: 0, config: map[NodeID]bool{0: true, 1: true, 2: true, 3: true, 4: true}}
	if got := five.majority(); got != 3 {
		t.Errorf("5-member majority = %d, want 3", got)
	}
	if got := five.votingPeers(); !reflect.DeepEqual(got, []NodeID{1, 2, 3, 4}) {
		t.Errorf("5-member votingPeers = %v, want [1 2 3 4]", got)
	}

	// Shrink to three members (as removing two servers would): majority drops to 2.
	three := &Node{id: 0, config: map[NodeID]bool{0: true, 1: true, 2: true}}
	if got := three.majority(); got != 2 {
		t.Errorf("3-member majority = %d, want 2", got)
	}
	if got := three.votingPeers(); !reflect.DeepEqual(got, []NodeID{1, 2}) {
		t.Errorf("3-member votingPeers = %v, want [1 2]", got)
	}

	// A member set that excludes this node (id 4 removed while it is still id 4)
	// yields an empty voting-peer list and a quorum of the remaining members.
	removedSelf := &Node{id: 4, config: map[NodeID]bool{0: true, 1: true, 2: true}}
	if got := removedSelf.majority(); got != 2 {
		t.Errorf("majority after self-removal = %d, want 2", got)
	}
	if got := removedSelf.votingPeers(); !reflect.DeepEqual(got, []NodeID{0, 1, 2}) {
		t.Errorf("votingPeers after self-removal = %v, want [0 1 2]", got)
	}

	// nil config falls back to the peer set.
	bare := &Node{id: 0, peers: []NodeID{1, 2}}
	if got := bare.majority(); got != 2 {
		t.Errorf("nil-config majority = %d, want 2", got)
	}
	if got := bare.votingPeers(); !reflect.DeepEqual(got, []NodeID{1, 2}) {
		t.Errorf("nil-config votingPeers = %v, want [1 2]", got)
	}
}
