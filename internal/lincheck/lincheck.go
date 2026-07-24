// Package lincheck is a from-scratch, zero-dependency linearizability checker
// for a key/value register. Given a history of concurrent client operations —
// each recording what was invoked, what was observed, and a real-time window
// (call tick, return tick) — it decides whether some sequential ordering of the
// operations is consistent with a KV specification AND with the real-time order
// (an operation that returned before another was called must be ordered before
// it).
//
// The search is the Wing & Gong linearization algorithm with the backtracking
// and memoization refinements popularized by Porcupine: provisionally linearize
// a pending operation the model accepts, recurse, and undo on a dead end,
// pruning states already proven unreachable. The problem is NP-complete in
// general, so we exploit KV structure: operations on distinct keys are
// independent, so the history is partitioned by key and each single-register
// sub-history is checked on its own. That keeps each search small in practice.
//
// The checker is deliberately independent of the cluster: it consumes a plain
// []Op and knows nothing about Raft. The harness that drives a real cluster and
// records histories lives in package cluster and feeds its results here. Keeping
// the algorithm separate means it can be trusted on its own — see the self-test
// battery in lincheck_test.go, which pins the verdict on hand-built histories
// with known answers ("who checks the checker").
package lincheck

import (
	"math"
	"sort"
)

// Infinity is the return tick of an operation that never completed: one still
// in flight when the cluster was torn down, or a write whose fate is unknown
// (an ambiguous error such as a leadership change or timeout that may have
// committed anyway). Such an operation may be linearized at any point at or
// after its call — including after every other operation — so it never forces a
// violation on its own, but it is still available to justify a later read.
const Infinity int64 = math.MaxInt64

// OpKind is the kind of KV operation.
type OpKind int

const (
	// Put writes Value at Key. Its result is always success, so it carries no
	// observation to validate — only an effect on the register.
	Put OpKind = iota
	// Get reads Key. Found reports whether a value was observed and, if so,
	// Value is that value. A Get is the only operation the model can reject: it
	// is legal exactly when the observed (Found, Value) matches the register.
	Get
	// Delete removes Key. Like Put its result is always success; it drives the
	// register to absent.
	Delete
)

// Op is one client operation and its observed outcome, tagged with the
// real-time window in which it executed. Call and Return come from a single
// monotonic tick counter shared by every client, so they impose the real-time
// partial order across clients: if a.Return < b.Call then a happened-before b
// and must be linearized first. Return is Infinity for an in-flight or
// fate-unknown operation.
//
// Only operations that definitely took effect belong in a history. A write that
// definitely did NOT commit (e.g. a first-committer-wins conflict) must be
// omitted; a write whose outcome is unknown is included with Return == Infinity.
// A read that errored is simply dropped — an unobserved read constrains nothing.
type Op struct {
	Client int
	Kind   OpKind
	Key    string
	Value  string // Put: written value. Get: observed value, valid iff Found.
	Found  bool   // Get: whether the key was observed present.
	Call   int64
	Return int64
}

// regState is the state of a single key's register: a value and whether the key
// is present. It is a comparable value type, so the memoization cache can key on
// it directly and compare with ==.
type regState struct {
	value   string
	present bool
}

// stepRegister applies op to a single-register state, returning the next state
// and whether the observation was legal. Put and Delete are always legal and
// only mutate; Get mutates nothing and is legal iff its observation matches.
func stepRegister(s regState, op Op) (regState, bool) {
	switch op.Kind {
	case Put:
		return regState{value: op.Value, present: true}, true
	case Delete:
		return regState{}, true
	case Get:
		if op.Found != s.present {
			return s, false
		}
		if op.Found && op.Value != s.value {
			return s, false
		}
		return s, true
	default:
		panic("lincheck: unknown op kind")
	}
}

// Result reports the outcome of a linearizability check. Linearizable is the
// verdict; on a violation, Key names the register whose sub-history could not be
// linearized, so a caller can print just the relevant operations.
type Result struct {
	Linearizable bool
	Key          string // set when !Linearizable: the offending key
}

// Check reports whether history is linearizable. It partitions the operations
// by key — sound because operations on distinct keys are independent in a KV —
// and checks each single-register sub-history separately. The first
// non-linearizable partition determines the verdict. An empty history is
// vacuously linearizable.
//
// history is not mutated; Check copies what it needs.
func Check(history []Op) Result {
	byKey := make(map[string][]Op)
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	// Deterministic order so a failing run reports the same key every time.
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if !linearizableRegister(byKey[k]) {
			return Result{Linearizable: false, Key: k}
		}
	}
	return Result{Linearizable: true}
}
