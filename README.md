# graceful

[![Go Reference](https://pkg.go.dev/badge/github.com/r4d01/graceful.svg)](https://pkg.go.dev/github.com/r4d01/graceful)

A `context.Context` that notices you before it cancels, so your code gets a
chance to finish up instead of being cut off.

## Install

```
go get github.com/r4d01/graceful
```

## The problem

- A plain `context.Context` only gives you one signal: `Done()`. It's either
  fine, or it's over. There's no "start wrapping up, you have 10 seconds"
  in between.
- `graceful.Context` adds that missing signal:
  - it **triggers shutdown** first, closing a channel you can watch for
  - then, one grace period later, it **cancels** — unless your work
    finishes first
- Shutdown can be triggered by any of:
  - an OS signal (SIGTERM by default)
  - a deadline, worked out automatically from the parent context's deadline
  - a manual call to `TriggerShutdown()`

## Usage

```go
gctx := graceful.NewContext(context.Background(), 10*time.Second, syscall.SIGTERM)

go func() {
	<-gctx.Shutdown() // SIGTERM received, or the deadline below is 10s out
	log.Println("shutting down, draining requests...")
}()

srv.Serve(listener)

<-gctx.Done() // cancelled 10s after shutdown starts, or sooner once drained
```

Skip the signal list to watch SIGTERM by default:

```go
gctx := graceful.NewContext(context.Background(), 10*time.Second)
```

If the parent already has a deadline, `graceful.Context` triggers shutdown
early enough to still finish its grace period by then:

```go
parent, cancel := context.WithTimeout(context.Background(), time.Minute)
defer cancel()

gctx := graceful.NewContext(parent, 10*time.Second)
// shutdown triggers at 50s, cancels at 60s - the parent's deadline is respected either way
```

### Go

`Go` runs a function on its own `graceful.Context` and returns a func to
trigger its shutdown manually. The context is cancelled as soon as the
function returns:

```go
triggerShutdown := graceful.Go(context.Background(), func(ctx context.Context) {
	<-ctx.Done()
}, 5*time.Second, syscall.SIGTERM)

// later, e.g. from a health check or admin endpoint:
triggerShutdown()
```

### Telling shutdown from a wrapped context

Once a `graceful.Context` has been wrapped - by `context.WithValue`,
`context.WithTimeout`, middleware, whatever - a type assertion back to
`*graceful.Context` no longer works. Use the package-level `Shutdown`
instead; it looks the channel up as a context value, so it survives
wrapping. It returns nil for a context that isn't graceful, so a bare
receive on it would block forever - check with `Shutdownable` first:

```go
func handler(ctx context.Context) {
	if !graceful.Shutdownable(ctx) {
		return // not a graceful context
	}
	select {
	case <-graceful.Shutdown(ctx):
		// shutdown has started
	default:
	}
}
```

### Telling *why* it cancelled

```go
<-gctx.Done()
switch {
case errors.Is(context.Cause(gctx), graceful.ErrGracePeriodExpired):
	// grace period ran out
case errors.Is(context.Cause(gctx), graceful.ErrCanceled):
	// Cancel was called
default:
	// the parent was cancelled
}
```

## Example

[`examples/simplecli`](examples/simplecli) is a small CLI that runs up to
10 tasks, one at a time. Run it, then press Ctrl+C: it stops picking up new
tasks, finishes the one in flight, and exits - all within a grace period.
Use `-p` and `-t` to control the grace period and how long each task takes.

```
go run ./examples/simplecli
```

## API

| | |
|---|---|
| `NewContext(parent, gracePeriod, signals...)` | create a `*Context` |
| `(*Context).TriggerShutdown()` | start the grace period now |
| `(*Context).Shutdown()` | channel closed when the grace period starts |
| `(*Context).Cancel()` | cancel immediately, no grace period |
| `(*Context).ScheduledShutdown()` | when it triggers shutdown on account of the parent's deadline |
| `Shutdown(ctx)` | `(*Context).Shutdown()`, but works through wrapped contexts |
| `Shutdownable(ctx)` | reports whether ctx is a graceful context |
| `Go(parent, fn, gracePeriod, signals...)` | run `fn` on a new `*Context`; returns its `TriggerShutdown` |
| `ErrGracePeriodExpired`, `ErrCanceled` | `context.Cause` values |

`*Context` also implements `context.Context` (`Deadline`, `Done`, `Err`,
`Value`), so it drops into anything that already accepts one.

Passing no signals to `NewContext` or `Go` watches `SIGTERM`; passing an
explicit empty slice watches none.

## License

Apache 2.0 - see [LICENSE](LICENSE).
