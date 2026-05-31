// Command littledb is the interactive REPL for the littledb key-value store.
//
// Usage:
//
//	littledb [-dir path] [-no-sync] [-memtable-size bytes]
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
	noSync := flag.Bool("no-sync", false, "skip fsync on every write (faster but loses tail on power loss)")
	memSize := flag.Int64("memtable-size", 0, "memtable byte threshold before flush (0 = default)")
	flag.Parse()

	opts := db.DefaultOptions()
	opts.SyncOnWrite = !*noSync
	if *memSize > 0 {
		opts.MemtableSizeMax = *memSize
	}

	d, err := db.OpenWith(*dir, opts)
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

	fmt.Printf("littledb — data dir: %s (sync=%v, memtable=%d) (type HELP for commands)\n",
		*dir, opts.SyncOnWrite, opts.MemtableSizeMax)
	if err := repl.Run(ctx, d, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "littledb: %v\n", err)
		os.Exit(1)
	}
}
