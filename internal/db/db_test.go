package db

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestPutGet(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if err := d.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	got, err := d.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("got %q, want %q", got, "world")
	}
}

func TestGetMissing(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()

	_, err := d.Get([]byte("nope"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err = %v, want ErrKeyNotFound", err)
	}
}

func TestOverwrite(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()

	d.Put([]byte("k"), []byte("v1"))
	d.Put([]byte("k"), []byte("v2"))
	d.Put([]byte("k"), []byte("v3"))

	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v3")) {
		t.Errorf("got %q, want v3", got)
	}
}

func TestDelete(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()

	d.Put([]byte("k"), []byte("v"))
	if err := d.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	_, err := d.Get([]byte("k"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("after delete, err = %v, want ErrKeyNotFound", err)
	}
}

func TestDeleteMissingKey(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()

	if err := d.Delete([]byte("never-existed")); err != nil {
		t.Errorf("delete missing: %v", err)
	}
}

func TestPutDeletePut(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()

	d.Put([]byte("k"), []byte("v1"))
	d.Delete([]byte("k"))
	d.Put([]byte("k"), []byte("v2"))

	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("got %q, want v2", got)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	d, _ := Open(dir)
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Put([]byte("c"), []byte("3"))
	d.Close()

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	cases := []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}}
	for _, c := range cases {
		got, err := d2.Get([]byte(c.k))
		if err != nil {
			t.Errorf("get %q after reopen: %v", c.k, err)
			continue
		}
		if !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("get %q: got %q, want %q", c.k, got, c.v)
		}
	}
}

func TestPersistenceWithOverwrites(t *testing.T) {
	dir := t.TempDir()

	d, _ := Open(dir)
	d.Put([]byte("k"), []byte("v1"))
	d.Put([]byte("k"), []byte("v2"))
	d.Put([]byte("k"), []byte("v3"))
	d.Close()

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	got, err := d2.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v3")) {
		t.Errorf("after reopen: got %q, want v3", got)
	}
}

func TestPersistenceWithDeletes(t *testing.T) {
	dir := t.TempDir()

	d, _ := Open(dir)
	d.Put([]byte("keep"), []byte("yes"))
	d.Put([]byte("gone"), []byte("no"))
	d.Delete([]byte("gone"))
	d.Close()

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if got, err := d2.Get([]byte("keep")); err != nil {
		t.Errorf("keep: %v", err)
	} else if !bytes.Equal(got, []byte("yes")) {
		t.Errorf("keep: got %q, want yes", got)
	}

	if _, err := d2.Get([]byte("gone")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("gone: err = %v, want ErrKeyNotFound", err)
	}
}

func TestManyKeys(t *testing.T) {
	dir := t.TempDir()
	d, _ := Open(dir)

	const n = 1000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := []byte(fmt.Sprintf("value-for-%d", i))
		if err := d.Put(k, v); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	d.Close()

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		want := []byte(fmt.Sprintf("value-for-%d", i))
		got, err := d2.Get(k)
		if err != nil {
			t.Errorf("get %d: %v", i, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("get %d: got %q, want %q", i, got, want)
		}
	}
}

func TestConcurrentReads(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()

	const n = 100
	for i := 0; i < n; i++ {
		d.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}

	const readers = 8
	const readsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(readers)
	errCh := make(chan error, readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerGoroutine; i++ {
				k := []byte(fmt.Sprintf("k%d", i%n))
				want := []byte(fmt.Sprintf("v%d", i%n))
				got, err := d.Get(k)
				if err != nil {
					errCh <- fmt.Errorf("get %s: %w", k, err)
					return
				}
				if !bytes.Equal(got, want) {
					errCh <- fmt.Errorf("get %s: got %q, want %q", k, got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()

	const n = 50
	for i := 0; i < n; i++ {
		d.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d-init", i)))
	}

	const readers = 4
	const writes = 500
	stop := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(readers + 1)
	errCh := make(chan error, readers+1)

	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < n; i++ {
					k := []byte(fmt.Sprintf("k%d", i))
					if _, err := d.Get(k); err != nil {
						errCh <- fmt.Errorf("reader: get %s: %w", k, err)
						return
					}
				}
			}
		}()
	}

	go func() {
		defer wg.Done()
		for w := 0; w < writes; w++ {
			k := []byte(fmt.Sprintf("k%d", w%n))
			v := []byte(fmt.Sprintf("v%d-update-%d", w%n, w))
			if err := d.Put(k, v); err != nil {
				errCh <- fmt.Errorf("writer: put %s: %w", k, err)
				return
			}
		}
		close(stop)
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	d, _ := Open(t.TempDir())
	if err := d.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
}

func TestOperationsAfterClose(t *testing.T) {
	d, _ := Open(t.TempDir())
	d.Close()

	if err := d.Put([]byte("k"), []byte("v")); err == nil {
		t.Error("put after close: want error")
	}
	if _, err := d.Get([]byte("k")); err == nil {
		t.Error("get after close: want error")
	}
	if err := d.Delete([]byte("k")); err == nil {
		t.Error("delete after close: want error")
	}
}
