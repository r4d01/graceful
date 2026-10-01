// Command simplecli is a minimal example of graceful shutdown.
//
// It runs up to 10 tasks, one every -t. Press Ctrl+C (or send SIGTERM) and
// it stops picking up new tasks, finishes the one in flight, and exits -
// all within -p. Hit Ctrl+C twice and it exits immediately.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/r4d01/graceful"
)

const maxTasks = 10

func run(ctx context.Context, processTime time.Duration) (int, error) {
	n := 1
	for ; n <= maxTasks; n++ {
		fmt.Printf("[task %02d] started\n", n)
		startedAt := time.Now()

		select {
		case <-ctx.Done():
			fmt.Printf("[task %02d] interrupted, context canceled\n", n)
			return n, context.Cause(ctx)
		case <-time.After(processTime):
			fmt.Printf("[task %02d] done\n", n)
		case <-graceful.ShutdownNotice(ctx):
			remaining := (processTime - time.Since(startedAt)).Round(time.Millisecond)
			fmt.Printf("[task %02d] shutdown requested, finishing up (%v left)\n", n, remaining)
			select {
			case <-time.After(processTime - time.Since(startedAt)):
				fmt.Printf("[task %02d] done, within the grace period\n", n)
				return n, nil
			case <-ctx.Done():
				fmt.Printf("[task %02d] ran out of time\n", n)
				return n, context.Cause(ctx)
			}
		}
	}
	return n, nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "simplecli runs up to %d tasks, one at a time, and shuts down gracefully on Ctrl+C or SIGTERM.\n\n", maxTasks)
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}

	var gracePeriod, processTime time.Duration
	flag.DurationVar(&gracePeriod, "p", 2*time.Second, "time allowed to finish up after shutdown starts (0 = disabled)")
	flag.DurationVar(&processTime, "t", 2*time.Second, "time it takes to process one task")
	flag.Parse()

	fmt.Printf("gracePeriod=%v processTime=%v (see -h to change)\n\n", gracePeriod, processTime)

	ctx := context.Background()
	if gracePeriod > 0 {
		gctx, cancel := graceful.NewContext(ctx, gracePeriod, syscall.SIGINT, syscall.SIGTERM)
		defer cancel() // skipped by the os.Exit below, but the process is going away anyway
		ctx = gctx
	} else {
		fmt.Fprintf(os.Stderr, "warning: -p 0 disables the grace period; Ctrl+C exits immediately\n\n")
	}

	n, err := run(ctx, processTime)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: task %d interrupted and may be left in a dirty state: %v\n", n, err)
		os.Exit(1)
	}

	if n < maxTasks {
		fmt.Printf("stopped after %d of %d tasks (shutdown requested)\n", n, maxTasks)
	}
	fmt.Println("exiting cleanly")
}
