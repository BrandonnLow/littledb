package cluster

import (
	"testing"
	"time"
)

// TestReadBarrierRefusesStaleReadOnExLeader is the ReadIndex counterpart to
// TestStaleReadIsCaught: the exact scenario that produced a stale read through the
// raw local-read path must NOT produce one through the linearizable path. A client
// pinned to a partitioned ex-leader gets no stale value — the ex-leader cannot
// commit the barrier no-op (no quorum), so linearizableGet blocks rather than
// serving its stale local state, and once healed it resolves to ErrNotLeader. The
// real leader serves the fresh value.
func TestReadBarrierRefusesStaleReadOnExLeader(t *testing.T) {
	const n = 3
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), pt, soakConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Put([]byte("x"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	oldLeaderID := c.Leader()

	// Isolate the leader; the majority elects a successor at a higher term.
	pt.disconnect(oldLeaderID)
	var newLeader *Node
	waitFor(t, 5*time.Second, func() bool {
		ld, ok := c.currentLeader()
		if ok && ld.id != oldLeaderID {
			newLeader = ld
			return true
		}
		return false
	})

	// The new leader commits x=2 and serves it via a linearizable read (it has a
	// quorum: itself plus the remaining connected follower).
	if err := c.Put([]byte("x"), []byte("2")); err != nil {
		t.Fatalf("write via new leader: %v", err)
	}
	if got, err := newLeader.linearizableGet([]byte("x")); err != nil || string(got) != "2" {
		t.Fatalf("linearizableGet on new leader = (%q,%v), want 2", got, err)
	}

	exLeader := c.Node(int(oldLeaderID))
	// Sanity: the raw local read on the ex-leader IS stale (the anomaly the barrier
	// prevents).
	if got, err := exLeader.storeGet([]byte("x")); err != nil || string(got) != "1" {
		t.Fatalf("sanity: ex-leader raw storeGet = (%q,%v), want stale 1", got, err)
	}

	// The linearizable read on the ex-leader must not serve the stale value. It
	// blocks on the barrier (no quorum to commit the no-op).
	type res struct {
		v   []byte
		err error
	}
	done := make(chan res, 1)
	go func() {
		v, err := exLeader.linearizableGet([]byte("x"))
		done <- res{v, err}
	}()
	select {
	case r := <-done:
		if r.err == nil && string(r.v) == "1" {
			t.Fatalf("linearizable read on ex-leader served stale 1")
		}
	case <-time.After(750 * time.Millisecond):
		// Blocked on the barrier — correct: it refuses to serve without quorum.
	}

	// Heal: the ex-leader learns the higher term and steps down, so the pending
	// barrier resolves to an error, never to the stale value.
	pt.reconnect(oldLeaderID)
	select {
	case r := <-done:
		if r.err == nil && string(r.v) == "1" {
			t.Fatalf("after heal, linearizable read on ex-leader served stale 1")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ex-leader linearizable read did not resolve after heal")
	}
}
