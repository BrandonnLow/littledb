package cluster

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// raftConfigFileName holds the durable BASE configuration — the voting membership
// as of the Raft log's compaction base. The current configuration is derived from
// it plus the config entries surviving in the log, so it need only be rewritten
// when the base advances (compaction / snapshot install), not on every change.
const raftConfigFileName = "config"

// encodeConfig serializes a voting-member set as count(4) | id(8) each, ids sorted
// ascending. Sorting makes the bytes deterministic across nodes, so a config entry
// replicates byte-for-byte and the base-config file round-trips identically.
func encodeConfig(members map[NodeID]bool) []byte {
	ids := make([]NodeID, 0, len(members))
	for id, in := range members {
		if in {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	buf := make([]byte, 4+8*len(ids))
	binary.LittleEndian.PutUint32(buf, uint32(len(ids)))
	for i, id := range ids {
		binary.LittleEndian.PutUint64(buf[4+8*i:], uint64(int64(id)))
	}
	return buf
}

// decodeConfig parses an encodeConfig payload back into a member set. A short or
// empty slice yields an empty set.
func decodeConfig(data []byte) map[NodeID]bool {
	out := map[NodeID]bool{}
	if len(data) < 4 {
		return out
	}
	n := int(binary.LittleEndian.Uint32(data))
	off := 4
	for i := 0; i < n && off+8 <= len(data); i++ {
		out[NodeID(int64(binary.LittleEndian.Uint64(data[off:])))] = true
		off += 8
	}
	return out
}

// copyConfig returns an independent copy of a member set.
func copyConfig(c map[NodeID]bool) map[NodeID]bool {
	out := make(map[NodeID]bool, len(c))
	for k, v := range c {
		if v {
			out[k] = true
		}
	}
	return out
}

// configEqual reports whether two member sets are identical.
func configEqual(a, b map[NodeID]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// readBaseConfigFile loads the persisted base configuration. ok is false when no
// file exists yet (a fresh node uses the bootstrap config instead).
func readBaseConfigFile(path string) (members map[NodeID]bool, ok bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	return decodeConfig(data), true
}

// writeBaseConfigFile durably records the base configuration, crash-atomically
// (temp -> fsync -> rename -> fsync-dir), the same pattern applied.base and the
// sessions file use. It survives a restart even after the config entry that set it
// has been compacted out of the Raft log.
func writeBaseConfigFile(path string, members map[NodeID]bool, sync bool) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("cluster: write base config: %w", err)
	}
	if _, err := f.Write(encodeConfig(members)); err != nil {
		f.Close()
		return fmt.Errorf("cluster: write base config: %w", err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("cluster: sync base config: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cluster: close base config: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("cluster: rename base config: %w", err)
	}
	if sync {
		if err := syncDir(filepath.Dir(path)); err != nil {
			return fmt.Errorf("cluster: sync dir after base config: %w", err)
		}
	}
	return nil
}

// adoptConfigLocked sets the current voting configuration from a config entry's
// payload — called the instant such an entry is appended (leader or follower), so
// a node acts on the newest configuration in its log before it commits (Raft
// dissertation §4.1). Must hold raftMu.
func (n *Node) adoptConfigLocked(data []byte) {
	n.config = decodeConfig(data)
}

// deriveConfigLocked recomputes the current configuration as the payload of the
// highest-indexed config entry still in the log, or the base config if none
// survive above the compaction base. It is the truncation counterpart to adopt:
// when a conflicting suffix (possibly containing an uncommitted config entry) is
// dropped, the configuration reverts to what the remaining log implies. Must hold
// raftMu.
func (n *Node) deriveConfigLocked() {
	for i := n.log.lastIndex(); i > n.log.baseIndex; i-- {
		if n.log.kindAt(i) == EntryConfig {
			n.config = decodeConfig(n.log.entryAt(i))
			return
		}
	}
	n.config = copyConfig(n.baseConfig)
}
