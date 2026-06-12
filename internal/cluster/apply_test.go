package cluster

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// gateTransport wraps a ChannelTransport and withholds messages of one type to
// one target node until released, letting a test pin a follower between
// "appended" and "applied".
type gateTransport struct {
	*ChannelTransport
	target NodeID
	hold   MsgType

	mu      sync.Mutex
	holding bool
	held    []Message
}

func newGateTransport(target NodeID, hold MsgType) *gateTransport {
	return &gateTransport{
		ChannelTransport: NewChannelTransport(),
		target:           target,
		hold:             hold,
		holding:          true,
	}
}

func (g *gateTransport) Send(to NodeID, msg Message) error {
	g.mu.Lock()
	if g.holding && to == g.target && msg.Type == g.hold {
		g.held = append(g.held, msg)
		g.mu.Unlock()
		return nil
	}
	g.mu.Unlock()
	return g.ChannelTransport.Send(to, msg)
}

func (g *gateTransport) release() {
	g.mu.Lock()
	g.holding = false
	held := g.held
	g.held = nil
	g.mu.Unlock()
	for _, m := range held {
		_ = g.ChannelTransport.Send(g.target, m)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFollowerDefersApplyUntilCommit is the core of stage 2a: a follower
// appends an entry to its log when it receives it (and acks, letting the
// leader reach quorum and commit), but does not apply it to its memtable until
// the leader's commit index reaches it. We hold the MsgCommit destined for one
// follower and observe it stuck at "appended, not applied", then release it
// and observe the apply.
func TestFollowerDefersApplyUntilCommit(t *testing.T) {
	const n = 3
	const target = NodeID(1)

	gate := newGateTransport(target, MsgCommit)
	c, err := NewWithTransport(n, dirs(t, n), testOpts(), gate)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// The quorum here is the leader plus one follower's append-ack, so Put
	// returns even though node 1's commit is being held.
	if err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	// The leader has applied: readable on it immediately.
	if got, err := c.Get([]byte("k")); err != nil || string(got) != "v" {
		t.Fatalf("leader Get = (%q,%v), want v", got, err)
	}

	// The gated follower appends the entry (MsgReplicate is not held)...
	tn := c.Node(int(target))
	waitFor(t, time.Second, func() bool { return tn.lastIndex() >= 1 })

	// ...but must not apply it while its MsgCommit is withheld.
	if ai := tn.appliedIndex(); ai != 0 {
		t.Fatalf("gated follower appliedIndex = %d, want 0 (apply withheld)", ai)
	}
	if _, err := tn.DB().Get([]byte("k")); !errors.Is(err, db.ErrKeyNotFound) {
		t.Fatalf("gated follower Get err = %v, want ErrKeyNotFound (logged, not applied)", err)
	}

	// Release the held commit: the follower now applies and converges.
	gate.release()
	waitFor(t, time.Second, func() bool { return tn.appliedIndex() >= 1 })
	if got, err := tn.DB().Get([]byte("k")); err != nil || string(got) != "v" {
		t.Fatalf("after release, follower Get = (%q,%v), want v", got, err)
	}
}
