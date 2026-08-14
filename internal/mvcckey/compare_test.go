package mvcckey

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
)

// TestComparePrefixKeyBoundary is the regression guard for the bug Compare
// exists to fix: when one userKey is a strict prefix of another, the shorter
// key's timestamp suffix lines up against the longer key's data bytes, and a
// raw byte comparison reads those suffix bytes as if they were key bytes. The
// suffix begins 0xff for any ordinary timestamp, which outranks printable
// bytes, so bytes.Compare puts "cat" after "cats" — inverting the userKey
// order. The two comparisons must disagree here; that disagreement is the
// whole reason Compare is not just bytes.Compare.
func TestComparePrefixKeyBoundary(t *testing.T) {
	cat := Encode([]byte("cat"), 30)
	cats := Encode([]byte("cats"), 30)

	if got := bytes.Compare(cat, cats); got <= 0 {
		t.Fatalf("premise broken: bytes.Compare(cat, cats) = %d, want > 0 "+
			"(the raw-byte inversion is what Compare exists to correct)", got)
	}
	if got := Compare(cat, cats); got >= 0 {
		t.Errorf("Compare(cat, cats) = %d, want < 0 (\"cat\" < \"cats\" as userKeys)", got)
	}
	if got := Compare(cats, cat); got <= 0 {
		t.Errorf("Compare(cats, cat) = %d, want > 0 (antisymmetry)", got)
	}
}

// TestCompareOrdersUserKeyThenTimestampDescending pins the full contract on a
// set deliberately seeded with prefix pairs ("cat"/"cats", "a"/"ab"/"abc"):
// userKey ascending, and within one userKey, timestamp descending.
func TestCompareOrdersUserKeyThenTimestampDescending(t *testing.T) {
	type kv struct {
		key string
		ts  uint64
	}
	// Deliberately shuffled; sorting must recover exactly this listed order.
	want := []kv{
		{"a", 9}, {"a", 2},
		{"ab", 5},
		{"abc", 100}, {"abc", 1},
		{"cat", 30}, {"cat", 10},
		{"cats", 7},
	}
	shuffled := []kv{
		{"cat", 10}, {"abc", 1}, {"a", 2}, {"cats", 7},
		{"abc", 100}, {"ab", 5}, {"cat", 30}, {"a", 9},
	}

	encoded := make([][]byte, len(shuffled))
	for i, e := range shuffled {
		encoded[i] = Encode([]byte(e.key), e.ts)
	}
	sort.Slice(encoded, func(i, j int) bool { return Compare(encoded[i], encoded[j]) < 0 })

	for i, enc := range encoded {
		uk, ts, ok := Decode(enc)
		if !ok {
			t.Fatalf("position %d: Decode failed", i)
		}
		if string(uk) != want[i].key || ts != want[i].ts {
			t.Errorf("position %d: got (%q, %d), want (%q, %d)",
				i, uk, ts, want[i].key, want[i].ts)
		}
	}
}

// TestCompareRandomizedRespectsSemantics is the property test. Hand-picked keys
// are exactly how this bug survived the suite in the first place — every scan
// test used same-length keys, so no prefix pair was ever compared. Here the
// alphabet is tiny and the lengths vary, so prefix pairs occur constantly by
// construction. Sorting by Compare must always yield userKey ascending, then
// timestamp strictly descending within a userKey.
func TestCompareRandomizedRespectsSemantics(t *testing.T) {
	const alphabet = "ab"
	rng := rand.New(rand.NewSource(1))

	for round := 0; round < 200; round++ {
		n := 2 + rng.Intn(30)
		seen := map[string]bool{}
		encoded := make([][]byte, 0, n)
		for len(encoded) < n {
			key := make([]byte, 1+rng.Intn(4)) // short keys -> frequent prefixes
			for i := range key {
				key[i] = alphabet[rng.Intn(len(alphabet))]
			}
			ts := uint64(1 + rng.Intn(20))
			id := string(key) + "@" + string(rune('0'+ts))
			if seen[id] {
				continue // a duplicate (userKey, ts) is not a distinct version
			}
			seen[id] = true
			encoded = append(encoded, Encode(key, ts))
		}

		sort.Slice(encoded, func(i, j int) bool { return Compare(encoded[i], encoded[j]) < 0 })

		for i := 1; i < len(encoded); i++ {
			prevKey, prevTS, ok1 := Decode(encoded[i-1])
			curKey, curTS, ok2 := Decode(encoded[i])
			if !ok1 || !ok2 {
				t.Fatalf("round %d: Decode failed", round)
			}
			switch c := bytes.Compare(prevKey, curKey); {
			case c > 0:
				t.Fatalf("round %d, position %d: userKey went backwards: %q then %q",
					round, i, prevKey, curKey)
			case c == 0 && prevTS <= curTS:
				t.Fatalf("round %d, position %d: userKey %q timestamps not descending: %d then %d",
					round, i, curKey, prevTS, curTS)
			}
		}
	}
}

// TestCompareShortSliceFallback covers inputs too short to carry a timestamp
// suffix. They are not well-formed encoded keys, but the ordering must stay
// total rather than panic on a negative slice bound.
func TestCompareShortSliceFallback(t *testing.T) {
	short, other := []byte("abc"), []byte("abd")
	if got := Compare(short, other); got >= 0 {
		t.Errorf("Compare(short, other) = %d, want < 0", got)
	}
	if got := Compare(short, short); got != 0 {
		t.Errorf("Compare(short, short) = %d, want 0", got)
	}
	if got := Compare(nil, nil); got != 0 {
		t.Errorf("Compare(nil, nil) = %d, want 0", got)
	}
	// Mixed well-formed and malformed must still return without panicking.
	_ = Compare(short, Encode([]byte("abc"), 1))
}
