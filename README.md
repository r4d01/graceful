# `graceful` - a context that notices you before it cancels

[![Go Reference](https://pkg.go.dev/badge/github.com/r4d01/graceful.svg)](https://pkg.go.dev/github.com/r4d01/graceful)

A plain `context.Context` only gives you one signal: `Done()`. It's either
  fine, or it's over. There's no *"start wrapping up, you have 10 seconds"*
  in between.

`graceful.Context` adds that missing signal:
- it gives you a **graceful shutdown notification** first, closing a channel you can watch for
- then, one grace period later, it **cancels** — unless your work
  finishes first

Shutdown can be triggered by any of:
- an OS signal (SIGTERM by default)
- a deadline, worked out automatically from the parent context's deadline
- a manual call to `TriggerShutdown()`

## Install

```
go get github.com/r4d01/graceful
```

## Usage

### Basic usage

```go
gctx, cancel := graceful.NewContext(context.Background(), 10*time.Second, syscall.SIGINT, syscall.SIGTERM)
defer cancel()

srv := &http.Server{Addr: ":8080"}
go func() {
	<-gctx.ShutdownNotice()
	srv.Shutdown(gctx) // stop accepting new requests, drain in-flight ones until the grace period ends
}()

srv.ListenAndServe()
```

### Default signal

Skip the signal list to watch SIGTERM by default:

```go
gctx, cancel := graceful.NewContext(context.Background(), 10*time.Second)
defer cancel() // releases the Context and its goroutine
```

### Deadlines from a parent context

If the parent already has a deadline, `graceful.Context` triggers shutdown
early enough to still finish its grace period by then:

```go
parent, cancel := context.WithTimeout(context.Background(), time.Minute)
defer cancel()

gctx, cancelGctx := graceful.NewContext(parent, 10*time.Second)
defer cancelGctx()
// shutdown triggers at 50s, cancels at 60s - the parent's deadline is respected either way
```

If the grace period is longer than the remaining parent deadline, shutdown
starts immediately. The parent deadline remains the hard cancellation
deadline.

### Run a routine with its own graceful context

`Go` runs a function on its own goroutine, backed by a new `graceful.Context`,
and returns a func to trigger its shutdown manually. The context is
cancelled as soon as the function returns:

```go
shutdown := graceful.Go(context.Background(), func(ctx context.Context) {
	<-graceful.ShutdownNotice(ctx)
	// wrap up before ctx.Done() fires
}, 5*time.Second, syscall.SIGTERM)

// later, e.g. from a health check or admin endpoint:
shutdown(false)     // start the grace period and carry on
shutdown(true)      // ...or block until the function has returned
```

Calling `shutdown(true)` from inside the function itself deadlocks - it
would be waiting on itself.

### React to a shutdown notice from any context

Once a `graceful.Context` has been wrapped - by `context.WithValue`,
`context.WithTimeout`, middleware, whatever - a type assertion back to
`*graceful.Context` no longer works. `ShutdownNotice(ctx)` looks the
channel up as a context value instead, so it survives wrapping. Take any
`context.Context` and select on both it and `ctx.Done()` - no type
assertion, no feature check:

```go
func handler(ctx context.Context) {
	select {
	case <-graceful.ShutdownNotice(ctx):
		// shutdown has started, wrap up before ctx.Done() fires
	case <-ctx.Done():
		// out of time
	}
}
```

`handler` works just as well on a plain context. `ShutdownNotice` returns
nil for one, and a nil channel never fires, so that case is simply never
chosen and the select falls through to `ctx.Done()` as it always would.

The one place this needs care is a *bare* receive - `<-graceful.ShutdownNotice(ctx)`
on its own line blocks forever on a plain context. Select on `ctx.Done()`
alongside it, as above, or check `Shutdownable` first.

### Check whether a context supports graceful shutdown

The select above doesn't need it, but when you want to *ask* - to log it, to
pick between a draining path and an abrupt one, to skip registering a hook -
`Shutdownable` reports whether a plain `context.Context` is backed by a
`graceful.Context` anywhere in its chain:

```go
if graceful.Shutdownable(ctx) {
	// ctx supports graceful shutdown
}
```

### Telling why it cancelled

```go
<-gctx.Done()
switch {
case errors.Is(context.Cause(gctx), graceful.ErrGracePeriodExpired):
	// grace period ran out
case errors.Is(context.Cause(gctx), graceful.ErrCanceled):
	// the func NewContext returned was called
default:
	// the parent was cancelled
}
```

## Example - try it by yourself

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
| `NewContext(parent, gracePeriod, signals...)` | create a `*Context`; returns it and a func that cancels it immediately, no grace period |
| `(*Context).TriggerShutdown()` | start the grace period now |
| `(*Context).ShutdownNotice()` | channel closed when the grace period starts |
| `(*Context).ScheduledShutdown()` | when it triggers shutdown on account of the parent's deadline |
| `ShutdownNotice(ctx)` | `(*Context).ShutdownNotice()`, but works through wrapped contexts |
| `Shutdownable(ctx)` | reports whether ctx is a graceful context |
| `Go(parent, fn, gracePeriod, signals...)` | run `fn` on a new `*Context`; returns a `func(wait bool)` that triggers its shutdown |
| `ErrGracePeriodExpired`, `ErrCanceled` | `context.Cause` values |

`*Context` also implements `context.Context` (`Deadline`, `Done`, `Err`,
`Value`), so it drops into anything that already accepts one.

Passing no signals (`nil`, what you get by omitting the argument) to
`NewContext` or `Go` watches `SIGTERM`; passing an explicit non-nil empty
slice (`[]os.Signal{}`) watches none.

## License

MIT - see [LICENSE](LICENSE).
