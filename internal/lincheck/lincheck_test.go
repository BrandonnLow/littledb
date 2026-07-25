package lincheck

import (
	"math/rand"
	"os"
	"strings"
	"testing"
)

// Constructors keep the hand-built histories below readable. Ticks are explicit
// so each case pins an exact real-time interleaving; within a history every tick
// is distinct except Infinity (an unfinished / fate-unknown operation).

func put(client int, key, val string, call, ret int64) Op {
	return Op{Client: client, Kind: Put, Key: key, Value: val, Call: call, Return: ret}
}

func getVal(client int, key, val string, call, ret int64) Op {
	return Op{Client: client, Kind: Get, Key: key, Value: val, Found: true, Call: call, Return: ret}
}

func getAbsent(client int, key string, call, ret int64) Op {
	return Op{Client: client, Kind: Get, Key: key, Found: false, Call: call, Return: ret}
}

func del(client int, key string, call, ret int64) Op {
	return Op{Client: client, Kind: Delete, Key: key, Call: call, Return: ret}
}

func TestChecker(t *testing.T) {
	cases := []struct {
		name    string
		history []Op
		want    bool
	}{
		{
			name:    "empty history is vacuously linearizable",
			history: nil,
			want:    true,
		},
		{
			name: "sequential put then read-back",
			history: []Op{
				put(0, "x", "1", 0, 1),
				getVal(0, "x", "1", 2, 3),
			},
			want: true,
		},
		{
			name: "read of a value never written",
			history: []Op{
				getVal(0, "x", "7", 0, 1),
			},
			want: false,
		},
		{
			name: "fresh key reads absent",
			history: []Op{
				getAbsent(0, "x", 0, 1),
			},
			want: true,
		},
		{
			name: "lost write: sequential put then read sees absent",
			history: []Op{
				put(0, "x", "1", 0, 1),
				getAbsent(0, "x", 2, 3),
			},
			want: false,
		},
		{
			name: "stale read: read after a completed write sees the old value",
			history: []Op{
				put(0, "x", "1", 0, 1), // x := 1, fully before the read
				getAbsent(1, "x", 2, 3),
			},
			want: false,
		},
		{
			name: "real time forces an order the model rejects (sequential put,put,get)",
			history: []Op{
				put(0, "x", "1", 0, 1),
				put(0, "x", "2", 2, 3),
				getVal(0, "x", "1", 4, 5), // must come after both; x==2 here, not 1
			},
			want: false,
		},
		{
			name: "concurrent writes, read overlaps and sees one of them (reorderable)",
			history: []Op{
				put(0, "x", "1", 0, 5),
				put(1, "x", "2", 1, 6),
				getVal(2, "x", "1", 2, 3), // order: Put1, Get(1), Put2 — all within windows
			},
			want: true,
		},
		{
			name: "in-flight write justifies an overlapping read (Infinity return)",
			history: []Op{
				put(0, "x", "1", 0, Infinity), // never returned, but took effect
				getVal(1, "x", "1", 1, 2),
			},
			want: true,
		},
		{
			name: "in-flight write need not take effect (floats to the end)",
			history: []Op{
				put(0, "x", "9", 0, Infinity),
				getAbsent(1, "x", 1, 2), // sees initial absent; the write linearizes after it
			},
			want: true,
		},
		{
			name: "delete then read-absent",
			history: []Op{
				put(0, "x", "1", 0, 1),
				del(0, "x", 2, 3),
				getAbsent(0, "x", 4, 5),
			},
			want: true,
		},
		{
			name: "read-absent with no delete after a write",
			history: []Op{
				put(0, "x", "1", 0, 1),
				getAbsent(0, "x", 2, 3),
			},
			want: false,
		},
		{
			name: "classic: concurrent write, two reads straddle it, later read regresses",
			history: []Op{
				put(0, "x", "1", 0, 3),
				getVal(1, "x", "1", 1, 2), // reads 1 while the write is in flight — ok
				getAbsent(2, "x", 4, 5),   // after the write returned; must not regress to absent
			},
			want: false,
		},
		{
			name: "four ops requiring the right interleaving (backtracking)",
			history: []Op{
				put(0, "x", "1", 0, 5),
				put(1, "x", "2", 1, 8),
				getVal(2, "x", "1", 2, 3), // pins Put1 before Put2
				getVal(3, "x", "2", 6, 7), // after Get1 (real time); pins Put2 last
			},
			want: true,
		},
		{
			name: "multi-key independent: both sub-histories linearizable",
			history: []Op{
				put(0, "x", "1", 0, 1),
				put(1, "y", "2", 0, 1), // concurrent, different key
				getVal(0, "x", "1", 2, 3),
				getVal(1, "y", "2", 2, 3),
			},
			want: true,
		},
		{
			name: "multi-key: one key's sub-history is non-linearizable",
			history: []Op{
				put(0, "x", "1", 0, 1),
				put(1, "y", "2", 0, 1),
				getVal(0, "x", "1", 2, 3),
				getAbsent(1, "y", 4, 5), // y lost its write
			},
			want: false,
		},
		{
			name: "overwrite chain read at the right point",
			history: []Op{
				put(0, "x", "a", 0, 1),
				put(0, "x", "b", 2, 3),
				put(0, "x", "c", 4, 5),
				getVal(1, "x", "b", 6, 9), // concurrent with the read below
				put(0, "x", "d", 7, 8),    // reorderable: read sees b before d lands
			},
			want: false, // sequential a,b,c are done; by tick 6 x==c, a read of "b" is stale
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Check(tc.history)
			if got.Linearizable != tc.want {
				t.Errorf("Check() linearizable = %v, want %v (offending key %q)",
					got.Linearizable, tc.want, got.Key)
			}
		})
	}
}

// genSequential builds a random but strictly sequential (non-overlapping)
// history over a few keys, recording each operation's true observed result from
// a reference model. Because the operations never overlap, real time forces a
// single legal order, so the history is linearizable by construction — and any
// single wrong observation makes it non-linearizable.
func genSequential(rng *rand.Rand, n int) []Op {
	keys := []string{"a", "b", "c"}
	state := map[string]regState{}
	var history []Op
	var tick int64
	for i := 0; i < n; i++ {
		k := keys[rng.Intn(len(keys))]
		call := tick
		ret := tick + 1
		tick += 2
		switch rng.Intn(3) {
		case 0: // Put
			v := string(rune('0' + rng.Intn(10)))
			history = append(history, Op{Client: i, Kind: Put, Key: k, Value: v, Call: call, Return: ret})
			state[k] = regState{value: v, present: true}
		case 1: // Delete
			history = append(history, Op{Client: i, Kind: Delete, Key: k, Call: call, Return: ret})
			state[k] = regState{}
		default: // Get, observing the true current state
			s := state[k]
			history = append(history, Op{Client: i, Kind: Get, Key: k, Value: s.value, Found: s.present, Call: call, Return: ret})
		}
	}
	return history
}

// TestCheckerSequentialAlwaysLinearizable is a property test: any faithfully
// recorded sequential history must check as linearizable, over many seeds.
func TestCheckerSequentialAlwaysLinearizable(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 500; iter++ {
		history := genSequential(rng, 30)
		if got := Check(history); !got.Linearizable {
			t.Fatalf("iter %d: sequential history reported non-linearizable (key %q)", iter, got.Key)
		}
	}
}

// TestCheckerCorruptedReadNonLinearizable is the dual property: corrupting a
// single read's observation in an otherwise-faithful sequential history must be
// caught. In a forced order there is exactly one legal observation per read, so
// any different one is a genuine violation.
func TestCheckerCorruptedReadNonLinearizable(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	checked := 0
	for iter := 0; iter < 500; iter++ {
		history := genSequential(rng, 30)
		// Find a Get to corrupt.
		idx := -1
		for i := range history {
			if history[i].Kind == Get {
				idx = i
				break
			}
		}
		if idx < 0 {
			continue // no read this iteration; skip
		}
		// Flip the observation to one that is definitely wrong: present with a
		// sentinel value distinct from any generated value ('0'..'9').
		history[idx].Found = true
		history[idx].Value = "WRONG"
		if got := Check(history); got.Linearizable {
			t.Fatalf("iter %d: corrupted read at %d reported linearizable", iter, idx)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no histories exercised a corrupted read")
	}
}

// TestCheckWitness verifies the counterexample a violation reports: the offending
// key, that key's operations in call order, and a non-empty rendering — and that a
// linearizable result carries no witness.
func TestCheckWitness(t *testing.T) {
	res := Check([]Op{
		put(0, "x", "1", 0, 1),
		getAbsent(1, "x", 2, 3), // stale: x is 1 by now
	})
	if res.Linearizable {
		t.Fatal("expected non-linearizable")
	}
	if res.Key != "x" {
		t.Fatalf("witness key = %q, want x", res.Key)
	}
	if len(res.Witness) != 2 {
		t.Fatalf("witness has %d ops, want 2", len(res.Witness))
	}
	if res.Witness[0].Call > res.Witness[1].Call {
		t.Fatal("witness is not call-ordered")
	}
	if FormatWitness(res.Witness) == "" {
		t.Fatal("FormatWitness returned empty")
	}

	if r := Check([]Op{
		put(0, "x", "1", 0, 1),
		getVal(1, "x", "1", 2, 3),
	}); !r.Linearizable || r.Witness != nil {
		t.Fatalf("linearizable result: linearizable=%v witness=%v, want true/nil", r.Linearizable, r.Witness)
	}
}

// TestRenderHistoryHTML checks the timeline renderer produces a self-contained
// page and highlights the witness on a violation. Set LINCHECK_HTML_OUT to a path
// to also dump a sample rendering for eyeballing.
func TestRenderHistoryHTML(t *testing.T) {
	// A realistic-looking stale read: a write completes, then a later read on a
	// different client misses it — the anomaly a partitioned ex-leader produces.
	bad := []Op{
		put(0, "x", "a", 0, 5),
		getVal(1, "x", "a", 6, 11),
		put(0, "x", "b", 12, 17),
		getVal(2, "x", "b", 18, 23),
		Op{Client: 3, Kind: Put, Key: "y", Value: "1", Call: 8, Return: Infinity}, // uncertain write
		getAbsent(1, "x", 30, 35), // stale: x is "b" by now
	}
	res := Check(bad)
	if res.Linearizable {
		t.Fatal("fixture should be non-linearizable")
	}
	html := RenderHistoryHTML(bad, res)
	// `witness"` matches the class APPLIED to an op group (not the CSS rule
	// `.bar.witness`), confirming the counterexample is highlighted.
	for _, want := range []string{"<!doctype html>", "<svg", "</svg>", "not linearizable", "pill bad", "witness\""} {
		if !strings.Contains(html, want) {
			t.Errorf("rendered HTML missing %q", want)
		}
	}

	// A linearizable history shows the ok verdict and highlights nothing.
	good := []Op{put(0, "x", "a", 0, 1), getVal(1, "x", "a", 2, 3)}
	if h := RenderHistoryHTML(good, Check(good)); !strings.Contains(h, "pill ok") || strings.Contains(h, "not linearizable") || strings.Contains(h, "witness\"") {
		t.Error("linearizable render should show the ok verdict and no applied witness class")
	}

	// Optionally dump a polished example fragment (LINCHECK_FRAG_OUT) or full page
	// (LINCHECK_HTML_OUT) for eyeballing / embedding.
	ex := []Op{
		put(0, "x", "A", 0, 6),
		getVal(1, "x", "A", 7, 12),
		{Client: 3, Kind: Put, Key: "y", Value: "1", Call: 10, Return: Infinity}, // uncertain write
		put(0, "x", "B", 14, 20),
		getVal(2, "x", "B", 21, 27),
		put(1, "x", "C", 30, 37),
		getVal(2, "x", "C", 40, 46),
		getVal(3, "y", "1", 48, 54),
		getVal(0, "x", "A", 50, 56), // STALE: x is "C" by now — the violation
	}
	exRes := Check(ex)
	if out := os.Getenv("LINCHECK_FRAG_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(RenderHistoryFragment(ex, exRes)), 0o644); err != nil {
			t.Fatalf("dump fragment: %v", err)
		}
		t.Logf("wrote fragment to %s", out)
	}
	if out := os.Getenv("LINCHECK_HTML_OUT"); out != "" {
		if err := os.WriteFile(out, []byte(RenderHistoryHTML(ex, exRes)), 0o644); err != nil {
			t.Fatalf("dump html: %v", err)
		}
		t.Logf("wrote page to %s", out)
	}
}

// TestCheckerManyConcurrentReads is a robustness/performance smoke test: one
// write followed by many fully-overlapping reads of that value. There are a
// huge number of valid interleavings but only a tiny reachable state space, so
// memoization must collapse it and the check must finish promptly.
func TestCheckerManyConcurrentReads(t *testing.T) {
	const readers = 200
	history := []Op{put(0, "x", "1", 0, 1)}
	for i := 0; i < readers; i++ {
		// All reads overlap in [10, 10_000] and all observe the written value.
		history = append(history, getVal(1+i, "x", "1", 10, 10_000))
	}
	if got := Check(history); !got.Linearizable {
		t.Fatalf("Check() = non-linearizable, want linearizable")
	}
}

// TestCheckerConcurrentDistinctWritesResolvable stresses backtracking: several
// concurrent writes of distinct values, with the final read pinning which write
// linearized last. Must resolve to linearizable.
func TestCheckerConcurrentDistinctWritesResolvable(t *testing.T) {
	const writers = 12
	var history []Op
	for i := 0; i < writers; i++ {
		history = append(history, put(i, "x", string(rune('a'+i)), 0, 1000))
	}
	// After all writes, a read observes the value written by the last writer.
	history = append(history, getVal(99, "x", string(rune('a'+writers-1)), 2000, 2001))
	if got := Check(history); !got.Linearizable {
		t.Fatalf("Check() = non-linearizable, want linearizable")
	}
}
