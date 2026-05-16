// Command littledb is the interactive REPL for the littledb key-value store.
//
// Usage:
//
//	littledb [-dir path]
//
// Type HELP at the prompt for available commands. Ctrl-D or EXIT to quit.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/BrandonnLow/littledb/internal/db"
	"github.com/BrandonnLow/littledb/internal/repl"
)

func main() {
	dir := flag.String("dir", "./data", "directory for database files")
	flag.Parse()

	d, err := db.Open(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "littledb: open %q: %v\n", *dir, err)
		os.Exit(1)
	}
	defer func() {
		if cerr := d.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "littledb: close: %v\n", cerr)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("littledb — data dir: %s (type HELP for commands)\n", *dir)
	if err := repl.Run(ctx, d, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "littledb: %v\n", err)
		os.Exit(1)
	}
}
