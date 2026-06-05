package mvcckey

import (
	"bytes"
	"sort"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, c := range []struct {
		userKey []byte
		ts      uint64
	}{
		{[]byte("hello"), 1},
		{[]byte("hello"), 1_000_000},
		{[]byte(""), 1},
		{[]byte("x"), 0},
		{[]byte("xxx"), ^uint64(0)},
	} {
		enc := Encode(c.userKey, c.ts)
		uk, ts, ok := Decode(enc)
		if !ok {
			t.Errorf("Decode(%v) ok=false", enc)
			continue
		}
		if !bytes.Equal(uk, c.userKey) {
			t.Errorf("userKey: got %q, want %q", uk, c.userKey)
		}
		if ts != c.ts {
			t.Errorf("ts: got %d, want %d", ts, c.ts)
		}
	}
}

// Encoded keys must sort by (userKey asc, ts desc).
func TestSortOrder(t *testing.T) {
	keys := [][]byte{
		Encode([]byte("a"), 100),
		Encode([]byte("a"), 300),
		Encode([]byte("a"), 200),
		Encode([]byte("b"), 50),
		Encode([]byte("b"), 75),
		Encode([]byte("c"), 999),
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	wantUserKeys := []string{"a", "a", "a", "b", "b", "c"}
	wantTS := []uint64{300, 200, 100, 75, 50, 999}
	for i, k := range keys {
		uk, ts, _ := Decode(k)
		if string(uk) != wantUserKeys[i] {
			t.Errorf("[%d] userKey = %q, want %q", i, uk, wantUserKeys[i])
		}
		if ts != wantTS[i] {
			t.Errorf("[%d] ts = %d, want %d", i, ts, wantTS[i])
		}
	}
}

// TestSeekTargetPositions confirms that for snapshot S, Encode(k, S)
// is the smallest encoded key that points at "version of k at or
// before S". A seek-greater-than-or-equal to that target lands on
// the right version.
func TestSeekTargetPositions(t *testing.T) {
	stored := [][]byte{
		Encode([]byte("k"), 10),
		Encode([]byte("k"), 20),
		Encode([]byte("k"), 30),
	}
	sort.Slice(stored, func(i, j int) bool {
		return bytes.Compare(stored[i], stored[j]) < 0
	})

	target := Encode([]byte("k"), 25)
	idx := sort.Search(len(stored), func(i int) bool {
		return bytes.Compare(stored[i], target) >= 0
	})
	if idx >= len(stored) {
		t.Fatal("no key found ≥ target")
	}
	_, gotTS, _ := Decode(stored[idx])
	if gotTS != 20 {
		t.Errorf("snapshot=25 landed on ts=%d, want 20", gotTS)
	}

	target = Encode([]byte("k"), 5)
	idx = sort.Search(len(stored), func(i int) bool {
		return bytes.Compare(stored[i], target) >= 0
	})
	if idx < len(stored) {
		_, gotTS, _ := Decode(stored[idx])
		t.Errorf("snapshot=5 should miss, but landed on ts=%d (idx=%d)", gotTS, idx)
	}

	target = Encode([]byte("k"), 100)
	idx = sort.Search(len(stored), func(i int) bool {
		return bytes.Compare(stored[i], target) >= 0
	})
	if idx >= len(stored) {
		t.Fatal("no key found at snapshot=100")
	}
	_, gotTS, _ = Decode(stored[idx])
	if gotTS != 30 {
		t.Errorf("snapshot=100 landed on ts=%d, want 30", gotTS)
	}
}

func TestUserKeyAndTimestampAccessors(t *testing.T) {
	enc := Encode([]byte("foo"), 42)
	if !bytes.Equal(UserKey(enc), []byte("foo")) {
		t.Errorf("UserKey: got %q", UserKey(enc))
	}
	if Timestamp(enc) != 42 {
		t.Errorf("Timestamp: got %d, want 42", Timestamp(enc))
	}
}

func TestDecodeTooShort(t *testing.T) {
	if _, _, ok := Decode([]byte{1, 2, 3}); ok {
		t.Error("Decode of short input returned ok=true")
	}
	if Timestamp([]byte{1, 2, 3}) != 0 {
		t.Error("Timestamp of short input should be 0 sentinel")
	}
	if UserKey([]byte{1, 2, 3}) != nil {
		t.Error("UserKey of short input should be nil")
	}
}

func TestEmptyUserKey(t *testing.T) {
	enc := Encode(nil, 7)
	if len(enc) != TimestampSize {
		t.Errorf("len = %d, want %d", len(enc), TimestampSize)
	}
	uk, ts, ok := Decode(enc)
	if !ok || len(uk) != 0 || ts != 7 {
		t.Errorf("decode: uk=%q ts=%d ok=%v", uk, ts, ok)
	}
}
