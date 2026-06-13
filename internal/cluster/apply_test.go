package cluster

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// gateTransport wraps a ChannelTransport and withholds every message matching
// a predicate until released, letting a test pin nodes in chosen states.
type gateTransport struct {
	*ChannelTransport
	pred func(to NodeID, m Message) bool

	mu      sync.Mutex
	holding bool
	held    []heldMsg
}

type heldMsg struct {
	to NodeID
	m  Message
}

func newGateTransport(pred func(to NodeID, m Message) bool) *gateTransport {
	return &gateTransport{
		ChannelTransport: NewChannelTransport(),
		pred:             pred,
		holding:          true,
	}
}

func (g *gateTransport) Send(to NodeID, msg Message) error {
	g.mu.Lock()
	if g.holding && g.pred(to, msg) {
		g.held = append(g.held, heldMsg{to, msg})
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
	for _, h := range held {
		_ = g.ChannelTransport.Send(h.to, h.m)
	}
}

func (g *gateTransport) setHolding(v bool) {
	g.mu.Lock()
	g.holding = v
	g.mu.Unlock()
}

func (g *gateTransport) heldCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.held)
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

// TestFollowerDefersApplyUntilCommit is the core of the deferred-apply split.
// Holding every follower response pins the leader's commit index at 0, so its
// commit blocks — yet the followers still append the entry to their logs on
// receipt (the AppendEntries carries leaderCommit 0). None of the three nodes
// applies until the responses are released and the commit index advances.
func TestFollowerDefersApplyUntilCommit(t *testing.T) {
	const n = 3
	gate := newGateTransport(func(to NodeID, m Message) bool {
		return to == 0 && m.Type == MsgAppendResponse
	})
	c, err := NewWithTransport(n, dirs(t, n), testOpts(), gate)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Commit in the background; it will block until a quorum responds.
	done := make(chan error, 1)
	go func() { done <- c.Put([]byte("k"), []byte("v")) }()

	// Both followers append the entry but, with leaderCommit pinned at 0, must
	// not apply it.
	for _, id := range []int{1, 2} {
		nd := c.Node(id)
		waitFor(t, time.Second, func() bool { return nd.lastIndex() >= 1 })
		if ai := nd.appliedIndex(); ai != 0 {
			t.Fatalf("follower %d appliedIndex = %d, want 0 (apply deferred)", id, ai)
		}
		if _, err := nd.DB().Get([]byte("k")); !errors.Is(err, db.ErrKeyNotFound) {
			t.Fatalf("follower %d Get err = %v, want ErrKeyNotFound (logged, not applied)", id, err)
		}
	}
	// The leader has it logged too and is blocked pre-apply.
	if ai := c.Node(0).appliedIndex(); ai != 0 {
		t.Fatalf("leader appliedIndex = %d, want 0 (commit blocked on quorum)", ai)
	}
	select {
	case <-done:
		t.Fatal("Put returned before any follower response was delivered")
	default:
	}

	// Release: the commit index advances and every node applies.
	gate.release()
	if err := <-done; err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if got, err := c.Node(i).DB().Get([]byte("k")); err != nil || string(got) != "v" {
			t.Errorf("node %d after release: Get = (%q,%v), want v", i, got, err)
		}
	}
}
