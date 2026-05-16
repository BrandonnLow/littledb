// Package repl implements a simple read-eval-print loop for littledb.
//
// It reads whitespace-separated commands from an io.Reader, dispatches
// them to a DB, and writes responses to an io.Writer. Splitting this
// into its own package keeps cmd/littledb/main.go small and makes the
// command logic unit-testable without spawning a process.
package repl

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/BrandonnLow/littledb/internal/db"
)

// Store is the subset of *db.DB the REPL uses. Defining an interface
// lets tests pass a fake without opening a real database.
type Store interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}

// Run reads commands from in line by line, calls the appropriate Store
// method, and writes results to out. It returns when in reaches EOF,
// when an EXIT command is read, or when ctx is cancelled.
//
// Recognized commands (case-insensitive):
//
//	PUT <key> <value>
//	GET <key>
//	DELETE <key>
//	EXIT | QUIT
//
// Errors from Store are reported to out and the loop continues; only
// I/O errors on in/out cause Run to return early.
func Run(ctx context.Context, store Store, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	// Allow long lines (default limit is 64KB). 1 MiB is plenty for a REPL
	// and matches what we tested in the WAL.
	scanner.Buffer(make([]byte, 0, 4096), 1<<20)

	writePrompt(out)
	for scanner.Scan() {
		// Check for cancellation between commands.
		select {
		case <-ctx.Done():
			fmt.Fprintln(out)
			return nil
		default:
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			writePrompt(out)
			continue
		}

		done := dispatch(store, line, out)
		if done {
			return nil
		}
		writePrompt(out)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("repl: read input: %w", err)
	}
	// Clean EOF (e.g. Ctrl-D). Print a newline so the shell prompt that
	// follows isn't glued to our last prompt.
	fmt.Fprintln(out)
	return nil
}

func writePrompt(out io.Writer) {
	fmt.Fprint(out, "> ")
}

// dispatch parses one line and runs the corresponding command.
// Returns true when the REPL should exit.
func dispatch(store Store, line string, out io.Writer) (exit bool) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return false
	}
	cmd := strings.ToUpper(fields[0])
	args := fields[1:]

	switch cmd {
	case "PUT":
		if len(args) != 2 {
			fmt.Fprintln(out, "usage: PUT <key> <value>")
			return false
		}
		if err := store.Put([]byte(args[0]), []byte(args[1])); err != nil {
			fmt.Fprintf(out, "ERROR: %v\n", err)
			return false
		}
		fmt.Fprintln(out, "OK")

	case "GET":
		if len(args) != 1 {
			fmt.Fprintln(out, "usage: GET <key>")
			return false
		}
		val, err := store.Get([]byte(args[0]))
		if err != nil {
			if errors.Is(err, db.ErrKeyNotFound) {
				fmt.Fprintln(out, "(not found)")
				return false
			}
			fmt.Fprintf(out, "ERROR: %v\n", err)
			return false
		}
		fmt.Fprintf(out, "%s\n", val)

	case "DELETE", "DEL":
		if len(args) != 1 {
			fmt.Fprintln(out, "usage: DELETE <key>")
			return false
		}
		if err := store.Delete([]byte(args[0])); err != nil {
			fmt.Fprintf(out, "ERROR: %v\n", err)
			return false
		}
		fmt.Fprintln(out, "OK")

	case "EXIT", "QUIT":
		return true

	case "HELP", "?":
		fmt.Fprintln(out, "commands:")
		fmt.Fprintln(out, "  PUT <key> <value>")
		fmt.Fprintln(out, "  GET <key>")
		fmt.Fprintln(out, "  DELETE <key>")
		fmt.Fprintln(out, "  EXIT")

	default:
		fmt.Fprintf(out, "unknown command: %s (try HELP)\n", fields[0])
	}
	return false
}
