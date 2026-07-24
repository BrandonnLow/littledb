package lincheck

import "sort"

// node is one endpoint (a call or a return) of an operation in the intrusive
// doubly-linked event list the search walks. Call nodes carry the operation and
// a match pointer to their return node; return nodes have match == nil. The list
// is threaded in real-time (tick) order behind a head sentinel and nil-
// terminated at the tail. prev/next of a node are never rewritten while it is
// lifted out — only its neighbours' pointers change — so unlift restores it
// exactly, which is what makes the backtracking sound.
type node struct {
	kind  entryKind
	id    int   // operation index, shared by a call and its return; indexes the bitset
	tick  int64 // real-time position: op.Call for a call, op.Return for a return
	op    Op    // valid on call nodes
	match *node // call -> its return; nil on a return node
	prev  *node
	next  *node
}

type entryKind uint8

const (
	entryCall entryKind = iota
	entryReturn
)

// buildEventList threads ops into a real-time-ordered doubly-linked list behind
// a head sentinel and returns the sentinel. Each op contributes a call node (at
// op.Call) and a return node (at op.Return); a stable sort by tick orders them,
// and since op.Call < op.Return for every op, a call always precedes its own
// return. The only ties are among Infinity returns, where order is irrelevant.
func buildEventList(ops []Op) *node {
	events := make([]*node, 0, 2*len(ops))
	for i, op := range ops {
		call := &node{kind: entryCall, id: i, tick: op.Call, op: op}
		ret := &node{kind: entryReturn, id: i, tick: op.Return}
		call.match = ret
		events = append(events, call, ret)
	}
	sort.SliceStable(events, func(a, b int) bool { return events[a].tick < events[b].tick })

	head := &node{}
	prev := head
	for _, e := range events {
		prev.next = e
		e.prev = prev
		prev = e
	}
	// prev.next stays nil: the tail is nil-terminated. The last event is always a
	// return (a call is always followed by its own later return), so advancing the
	// cursor past calls can never run off the end before hitting a return.
	return head
}

// lift splices a call node and its matching return out of the list, provisionally
// committing to linearizing that operation now. It rewrites only the neighbours'
// links, leaving the lifted nodes' own prev/next intact so unlift is an exact
// inverse — including when a call and its return are adjacent (a sequential op),
// where the specific assignment order below still composes correctly.
func lift(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev // a call always has a later return, so n.next is non-nil
	m := n.match
	m.prev.next = m.next
	if m.next != nil {
		m.next.prev = m.prev
	}
}

// unlift reinserts a previously lifted call and its return, undoing lift exactly.
func unlift(n *node) {
	m := n.match
	m.prev.next = m
	if m.next != nil {
		m.next.prev = m
	}
	n.prev.next = n
	n.next.prev = n
}

// bitset is a fixed-width set of linearized operation ids. It is the memoization
// key (with the model state): a (linearized-set, state) pair once explored is
// never worth exploring again, which is what tames the exponential search.
type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }

func (b bitset) set(i int)   { b[i>>6] |= 1 << uint(i&63) }
func (b bitset) clear(i int) { b[i>>6] &^= 1 << uint(i&63) }

func (b bitset) clone() bitset {
	c := make(bitset, len(b))
	copy(c, b)
	return c
}

// hash is an FNV-1a fold of the words, matching the hash/fnv choice used across
// the rest of the codebase. Collisions are resolved by equal() at the call site.
func (b bitset) hash() uint64 {
	const offset64 = 1469598103934665603
	const prime64 = 1099511628211
	h := uint64(offset64)
	for _, w := range b {
		h = (h ^ w) * prime64
	}
	return h
}

func (b bitset) equal(o bitset) bool {
	if len(b) != len(o) {
		return false
	}
	for i := range b {
		if b[i] != o[i] {
			return false
		}
	}
	return true
}

// cacheEntry is one explored frontier: a linearized-set and the register state
// it produced. Stored append-only, bucketed by the set's hash.
type cacheEntry struct {
	set   bitset
	state regState
}

func cacheHas(cache map[uint64][]cacheEntry, set bitset, st regState) bool {
	for _, e := range cache[set.hash()] {
		if e.state == st && e.set.equal(set) {
			return true
		}
	}
	return false
}

func cacheAdd(cache map[uint64][]cacheEntry, set bitset, st regState) {
	h := set.hash()
	cache[h] = append(cache[h], cacheEntry{set: set, state: st})
}

// frame is one entry on the backtracking stack: the call we provisionally
// linearized and the register state from before we did, so undo restores it.
type frame struct {
	call  *node
	state regState
}

// linearizableRegister decides whether a single key's sub-history admits a
// sequential ordering consistent with the register spec and real time. It is the
// Wing & Gong search: walk the event list; at a call the model accepts, tentatively
// linearize it (unless that exact frontier is cached), lift it out, and restart;
// reaching a return whose call is not yet linearized means the current prefix is a
// dead end, so undo the last choice and try the next. An empty list means every
// operation was placed — linearizable. An exhausted stack at a forced return means
// no ordering works.
func linearizableRegister(ops []Op) bool {
	n := len(ops)
	if n == 0 {
		return true
	}
	head := buildEventList(ops)

	linearized := newBitset(n)
	cache := make(map[uint64][]cacheEntry)
	calls := make([]frame, 0, n)
	state := regState{} // initial: key absent

	entry := head.next
	for head.next != nil {
		if entry.kind == entryCall {
			newState, ok := stepRegister(state, entry.op)
			if ok {
				tentative := linearized.clone()
				tentative.set(entry.id)
				if !cacheHas(cache, tentative, newState) {
					cacheAdd(cache, tentative, newState)
					calls = append(calls, frame{call: entry, state: state})
					linearized.set(entry.id)
					state = newState
					lift(entry)
					entry = head.next
					continue
				}
			}
			// Can't (or needn't) linearize here; try to place a later operation
			// first, deferring this one within its real-time window.
			entry = entry.next
		} else {
			// A return reached before its call was linearized: this operation had
			// to finish by now but no legal ordering has placed it. Backtrack.
			if len(calls) == 0 {
				return false
			}
			top := calls[len(calls)-1]
			calls = calls[:len(calls)-1]
			entry = top.call
			state = top.state
			linearized.clear(entry.id)
			unlift(entry)
			entry = entry.next
		}
	}
	return true
}
