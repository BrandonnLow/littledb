// Package skiplist implements a sorted in-memory map keyed by []byte.
//
// A skiplist is a probabilistic alternative to balanced trees: each node
// is promoted to higher levels with 50% probability per coin flip, giving
// O(log N) expected lookup, insert, and delete with no rebalancing.
//
// This skiplist is NOT safe for concurrent use. The memtable on top
// provides its own locking.
package skiplist

import (
	"bytes"
	"math/rand/v2"
)

// maxHeight bounds the height of any node. 32 levels supports ~2^32 entries
// before the expected lookup cost starts to degrade — comfortably more than
// any single memtable will ever hold.
const maxHeight = 32

// Node is one entry in the skiplist.
type Node struct {
	key   []byte
	value []byte
	// next[i] is the next node at level i. len(next) is this node's height.
	next []*Node
}

// Key returns the node's key. The returned slice aliases internal storage;
// callers must not modify it.
func (n *Node) Key() []byte { return n.key }

// Value returns the node's value. The returned slice aliases internal storage;
// callers must not modify it.
func (n *Node) Value() []byte { return n.value }

// Skiplist is a sorted map from []byte keys to []byte values.
type Skiplist struct {
	head   *Node // sentinel; head.next[i] is the first node at level i
	height int   // number of levels currently in use (1..maxHeight)
	length int   // number of real entries
}

// New returns an empty skiplist.
func New() *Skiplist {
	return &Skiplist{
		head:   &Node{next: make([]*Node, maxHeight)},
		height: 1,
	}
}

// Len returns the number of entries.
func (s *Skiplist) Len() int { return s.length }

// findPredecessors returns, for each level, the node immediately to the
// left of where key would sit. After this call, prev[i].next[i] is either
// the first node at level i with key >= the target, or nil.
//
// We use a fixed-size array (not a slice) to avoid per-call allocation.
func (s *Skiplist) findPredecessors(key []byte) [maxHeight]*Node {
	var prev [maxHeight]*Node
	cur := s.head
	for i := s.height - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		prev[i] = cur
	}
	return prev
}

// randomHeight returns a new node height. Height H is chosen with
// probability (1/2)^H, capped at maxHeight.
func randomHeight() int {
	h := 1
	for h < maxHeight && rand.Float64() < 0.5 {
		h++
	}
	return h
}

// Put inserts (key, value) or replaces an existing entry. Both slices are
// copied; the caller may reuse them after Put returns.
func (s *Skiplist) Put(key, value []byte) {
	prev := s.findPredecessors(key)

	// If the key already exists at level 0, update in place.
	if cand := prev[0].next[0]; cand != nil && bytes.Equal(cand.key, key) {
		// Allocate a fresh slice so any borrowed references to the old
		// value remain valid until they're dropped.
		cand.value = append([]byte(nil), value...)
		return
	}

	// New node. Pick a height; if it exceeds the current top level, the
	// extra slots in prev are head (which is what they should be — the
	// new node will be the first at those levels).
	h := randomHeight()
	if h > s.height {
		for i := s.height; i < h; i++ {
			prev[i] = s.head
		}
		s.height = h
	}

	n := &Node{
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
		next:  make([]*Node, h),
	}
	for i := 0; i < h; i++ {
		n.next[i] = prev[i].next[i]
		prev[i].next[i] = n
	}
	s.length++
}

// Get returns the value for key. ok is false if the key is not present.
// The returned slice aliases internal storage; callers must not modify it.
// It remains valid for the lifetime of the entry (until Delete or a Put
// with the same key reassigns its value).
func (s *Skiplist) Get(key []byte) (value []byte, ok bool) {
	cur := s.head
	for i := s.height - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
	}
	// cur is now the largest node with key < target (or head if no such).
	// The first candidate to inspect is cur.next[0].
	if cand := cur.next[0]; cand != nil && bytes.Equal(cand.key, key) {
		return cand.value, true
	}
	return nil, false
}

// Delete removes key. Returns true if the key was present.
func (s *Skiplist) Delete(key []byte) bool {
	prev := s.findPredecessors(key)
	target := prev[0].next[0]
	if target == nil || !bytes.Equal(target.key, key) {
		return false
	}
	for i := 0; i < len(target.next); i++ {
		prev[i].next[i] = target.next[i]
	}
	// Lower the height if the top levels are now empty.
	for s.height > 1 && s.head.next[s.height-1] == nil {
		s.height--
	}
	s.length--
	return true
}

// Iterate calls fn for each (key, value) in sorted order. Return false
// from fn to stop iterating early.
//
// The slices passed to fn alias internal storage. They are safe to read
// for as long as the entry exists. Treat them as read-only.
func (s *Skiplist) Iterate(fn func(key, value []byte) bool) {
	for cur := s.head.next[0]; cur != nil; cur = cur.next[0] {
		if !fn(cur.key, cur.value) {
			return
		}
	}
}
