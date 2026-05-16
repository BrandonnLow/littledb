package repl

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/BrandonnLow/littledb/internal/db"
)

// fakeStore is a tiny in-memory Store for testing the REPL in isolation
// from the real database.
type fakeStore struct {
	m map[string]string
}

func newFakeStore() *fakeStore { return &fakeStore{m: map[string]string{}} }

func (f *fakeStore) Put(key, value []byte) error {
	f.m[string(key)] = string(value)
	return nil
}

func (f *fakeStore) Get(key []byte) ([]byte, error) {
	v, ok := f.m[string(key)]
	if !ok {
		return nil, db.ErrKeyNotFound
	}
	return []byte(v), nil
}

func (f *fakeStore) Delete(key []byte) error {
	delete(f.m, string(key))
	return nil
}

func runREPL(t *testing.T, store Store, input string) string {
	t.Helper()
	var out bytes.Buffer
	if err := Run(context.Background(), store, strings.NewReader(input), &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return out.String()
}

func TestPutGet(t *testing.T) {
	out := runREPL(t, newFakeStore(), "PUT hello world\nGET hello\n")
	if !strings.Contains(out, "OK") {
		t.Errorf("expected OK in output, got: %q", out)
	}
	if !strings.Contains(out, "world") {
		t.Errorf("expected 'world' in output, got: %q", out)
	}
}

func TestGetMissing(t *testing.T) {
	out := runREPL(t, newFakeStore(), "GET nope\n")
	if !strings.Contains(out, "(not found)") {
		t.Errorf("expected '(not found)' in output, got: %q", out)
	}
}

func TestDelete(t *testing.T) {
	s := newFakeStore()
	s.Put([]byte("k"), []byte("v"))
	out := runREPL(t, s, "DELETE k\nGET k\n")
	if !strings.Contains(out, "OK") {
		t.Errorf("expected OK, got: %q", out)
	}
	if !strings.Contains(out, "(not found)") {
		t.Errorf("expected '(not found)' after delete, got: %q", out)
	}
}

func TestCommandsAreCaseInsensitive(t *testing.T) {
	out := runREPL(t, newFakeStore(), "put k v\nGet k\nDeL k\nget k\n")
	if !strings.Contains(out, "v") {
		t.Errorf("get should have returned v, got: %q", out)
	}
	if !strings.Contains(out, "(not found)") {
		t.Errorf("get after del should be (not found), got: %q", out)
	}
}

func TestUsageMessages(t *testing.T) {
	out := runREPL(t, newFakeStore(), "PUT only-one-arg\nGET\nDELETE\n")
	for _, expected := range []string{"usage: PUT", "usage: GET", "usage: DELETE"} {
		if !strings.Contains(out, expected) {
			t.Errorf("expected %q in output, got: %q", expected, out)
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	out := runREPL(t, newFakeStore(), "FROBNICATE x\n")
	if !strings.Contains(out, "unknown command") {
		t.Errorf("expected 'unknown command', got: %q", out)
	}
}

func TestExit(t *testing.T) {
	s := newFakeStore()
	out := runREPL(t, s, "PUT a 1\nEXIT\nPUT b 2\n")
	if !strings.Contains(out, "OK") {
		t.Errorf("first PUT should have succeeded, got: %q", out)
	}
	if _, ok := s.m["b"]; ok {
		t.Error("commands after EXIT should not execute")
	}
}

func TestQuitAlias(t *testing.T) {
	s := newFakeStore()
	runREPL(t, s, "PUT a 1\nQUIT\nPUT b 2\n")
	if _, ok := s.m["b"]; ok {
		t.Error("commands after QUIT should not execute")
	}
}

func TestHelp(t *testing.T) {
	out := runREPL(t, newFakeStore(), "HELP\n")
	for _, want := range []string{"PUT", "GET", "DELETE", "EXIT"} {
		if !strings.Contains(out, want) {
			t.Errorf("HELP missing %q: %q", want, out)
		}
	}
}

func TestEmptyLines(t *testing.T) {
	out := runREPL(t, newFakeStore(), "\n\n   \nPUT a 1\n")
	if !strings.Contains(out, "OK") {
		t.Errorf("expected OK after empty lines, got: %q", out)
	}
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var out bytes.Buffer
	err := Run(ctx, newFakeStore(), strings.NewReader("PUT a 1\nPUT b 2\n"), &out)
	if err != nil {
		t.Errorf("Run with cancelled ctx: %v", err)
	}
}
