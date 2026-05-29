package bloom

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestEmptyFilter(t *testing.T) {
	f := New(100, 10)
	if f.MayContain([]byte("anything")) {
		t.Error("empty filter: MayContain returned true")
	}
}

func TestNoFalseNegatives(t *testing.T) {
	// Non-negotiable property: every key that was Added must MayContain.
	f := New(10_000, 10)
	keys := make([][]byte, 10_000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%06d", i))
		f.Add(keys[i])
	}
	for i, k := range keys {
		if !f.MayContain(k) {
			t.Fatalf("false negative at index %d: %q", i, k)
		}
	}
}

func TestFalsePositiveRateNearTarget(t *testing.T) {
	// With 10 bits/key the theoretical FPR is ~1%. Add 10k keys, test
	// 100k absent keys.
	f := New(10_000, 10)
	for i := 0; i < 10_000; i++ {
		f.Add([]byte(fmt.Sprintf("added-%06d", i)))
	}

	const trials = 100_000
	falsePositives := 0
	for i := 0; i < trials; i++ {
		k := []byte(fmt.Sprintf("absent-%06d", i))
		if f.MayContain(k) {
			falsePositives++
		}
	}
	fpr := float64(falsePositives) / float64(trials)
	t.Logf("measured FPR = %.4f (%d/%d) at 10 bits/key, k=%d", fpr, falsePositives, trials, f.NumHashes())

	if fpr > 0.02 {
		t.Errorf("FPR %.4f exceeds 2%% tolerance band", fpr)
	}
	if fpr < 0.005 {
		t.Errorf("FPR %.4f below 0.5%% — suspiciously good, check the test", fpr)
	}
}

func TestFalsePositiveRateScalesWithBitsPerKey(t *testing.T) {
	cases := []struct {
		bitsPerKey int
		maxFPR     float64
	}{
		{4, 0.20},
		{8, 0.04},
		{16, 0.005},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("bits=%d", c.bitsPerKey), func(t *testing.T) {
			f := New(1000, c.bitsPerKey)
			for i := 0; i < 1000; i++ {
				f.Add([]byte(fmt.Sprintf("a-%04d", i)))
			}
			fp := 0
			const trials = 20_000
			for i := 0; i < trials; i++ {
				if f.MayContain([]byte(fmt.Sprintf("b-%04d", i))) {
					fp++
				}
			}
			fpr := float64(fp) / float64(trials)
			t.Logf("bits/key=%d k=%d FPR=%.4f", c.bitsPerKey, f.NumHashes(), fpr)
			if fpr > c.maxFPR {
				t.Errorf("FPR %.4f exceeds max %.4f for bits=%d", fpr, c.maxFPR, c.bitsPerKey)
			}
		})
	}
}

func TestSerializationRoundTrip(t *testing.T) {
	f := New(5000, 10)
	keys := make([][]byte, 5000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k-%05d", i))
		f.Add(keys[i])
	}

	data := f.Bytes()
	g, err := Load(data)
	if err != nil {
		t.Fatal(err)
	}

	if g.NumBits() != f.NumBits() {
		t.Errorf("NumBits: got %d want %d", g.NumBits(), f.NumBits())
	}
	if g.NumHashes() != f.NumHashes() {
		t.Errorf("NumHashes: got %d want %d", g.NumHashes(), f.NumHashes())
	}
	for _, k := range keys {
		if !g.MayContain(k) {
			t.Errorf("after Load, %q not found", k)
		}
	}
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("absent-%d", i))
		if f.MayContain(k) != g.MayContain(k) {
			t.Errorf("Load disagreement on %q", k)
		}
	}
}

func TestLoadBadInput(t *testing.T) {
	if _, err := Load(nil); err == nil {
		t.Error("Load(nil): expected error")
	}
	if _, err := Load([]byte{}); err == nil {
		t.Error("Load(empty): expected error")
	}
	if _, err := Load([]byte{0}); err == nil {
		t.Error("Load(k=0): expected error")
	}
	if _, err := Load([]byte{99}); err == nil {
		t.Error("Load(k=99): expected error")
	}
}

func TestDeterminism(t *testing.T) {
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%04d", i))
	}

	f1 := New(1000, 10)
	for _, k := range keys {
		f1.Add(k)
	}

	r := rand.New(rand.NewPCG(123, 456))
	shuf := make([][]byte, len(keys))
	copy(shuf, keys)
	r.Shuffle(len(shuf), func(i, j int) { shuf[i], shuf[j] = shuf[j], shuf[i] })

	f2 := New(1000, 10)
	for _, k := range shuf {
		f2.Add(k)
	}

	if !bytes.Equal(f1.Bytes(), f2.Bytes()) {
		t.Error("serialized filters differ for same keys in different order")
	}
}

func TestKComputation(t *testing.T) {
	cases := []struct {
		bitsPerKey int
		wantK      int
	}{
		{1, 1},
		{4, 3},
		{10, 7},
		{20, 14},
		{64, 30}, // clamped to 30
	}
	for _, c := range cases {
		f := New(100, c.bitsPerKey)
		if f.NumHashes() != c.wantK {
			t.Errorf("bits=%d: k=%d, want %d", c.bitsPerKey, f.NumHashes(), c.wantK)
		}
	}
}

func TestTinyAndDegenerate(t *testing.T) {
	f := New(0, 0)
	f.Add([]byte("k"))
	if !f.MayContain([]byte("k")) {
		t.Error("after Add, MayContain returned false")
	}

	f = New(-100, -5)
	f.Add([]byte("k"))
	if !f.MayContain([]byte("k")) {
		t.Error("negative inputs: after Add, MayContain false")
	}
}

func TestEmptyKey(t *testing.T) {
	f := New(100, 10)
	f.Add([]byte(""))
	if !f.MayContain([]byte("")) {
		t.Error("empty key: MayContain false after Add")
	}
}

func TestSerializationOfEmptyFilter(t *testing.T) {
	f := New(100, 10)
	data := f.Bytes()
	g, err := Load(data)
	if err != nil {
		t.Fatal(err)
	}
	if g.MayContain([]byte("x")) {
		t.Error("loaded empty filter: MayContain returned true")
	}
}
