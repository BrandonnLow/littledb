package cluster

import (
	"reflect"
	"testing"
)

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
