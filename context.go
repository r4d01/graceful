// Package graceful provides a context.Context that notices you before it
// cancels, so you get time to finish up.
package graceful

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"
)

// ErrGracePeriodExpired is the context.Cause of a Context whose grace period elapsed.
var ErrGracePeriodExpired = errors.New("graceful: grace period expired")

// ErrCanceled is the context.Cause of a Context cancelled by Cancel.
var ErrCanceled = errors.New("graceful: canceled")

var defaultSignals = []os.Signal{syscall.SIGTERM}

// shutdownKey carries the shutdown channel, so Shutdown works through wrapped contexts.
type shutdownKey struct{}

// Context is a context.Context that signals before it cancels. Create one with NewContext.
type Context struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	gracePeriod time.Duration

	shutdownTriggerC chan struct{}

	scheduledShutdownTriggerC    <-chan time.Time
	scheduledShutdownTriggerTime time.Time

	manualShutdownTriggerC <-chan struct{}
	manualShutdownTrigger  func()

	signalWatcher *signalWatcher
}

// scheduleShutdownTrigger returns a timer for gracePeriod before ctx's deadline,
// or nil if ctx has no deadline.
func scheduleShutdownTrigger(ctx context.Context, gracePeriod time.Duration) (<-chan time.Time, time.Time) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, time.Time{}
	}

	triggerTime := deadline.Add(-gracePeriod)
	return time.After(time.Until(triggerTime)), triggerTime
}

// NewContext creates a graceful context. Shutdown begins on any of the given
// signals, or gracePeriod before the parent's deadline. The Context is
// cancelled gracePeriod after shutdown begins.
//
// Passing no signals watches SIGTERM; passing an empty non-nil slice watches none.
//
// The Context and its goroutine live until it is cancelled: by Cancel, by the
// grace period, or by the parent.
func NewContext(parent context.Context, gracePeriod time.Duration, signals ...os.Signal) *Context {
	shutdownTriggerC := make(chan struct{})

	// A timer, unlike a signal, cannot be missed by starting late.
	scheduledShutdownTriggerC, scheduledShutdownTriggerTime := scheduleShutdownTrigger(parent, gracePeriod)

	manualShutdownTriggerC, manualShutdownTrigger := newTrigger()

	// Registered here, not in the goroutine: a signal arriving before the
	// goroutine runs would be missed, and would kill the process.
	signalWatcher := newSignalWatcher(signals)

	ctx, cancel := context.WithCancelCause(parent)
	ctx = context.WithValue(ctx, shutdownKey{}, (<-chan struct{})(shutdownTriggerC))

	gctx := &Context{
		ctx:    ctx,
		cancel: cancel,

		gracePeriod: gracePeriod,

		shutdownTriggerC: shutdownTriggerC,

		scheduledShutdownTriggerC:    scheduledShutdownTriggerC,
		scheduledShutdownTriggerTime: scheduledShutdownTriggerTime,

		manualShutdownTriggerC: manualShutdownTriggerC,
		manualShutdownTrigger:  manualShutdownTrigger,

		signalWatcher: signalWatcher}
	go gctx.awaitShutdownStart()
	return gctx
}

// awaitShutdownStart waits for the first shutdown trigger, then starts the grace period.
func (c *Context) awaitShutdownStart() {

	select {
	case <-c.ctx.Done():
		c.signalWatcher.stop()
		return
	case <-c.manualShutdownTriggerC:
	case <-c.scheduledShutdownTriggerC:
	case <-c.signalWatcher.c:
	}

	// Restore the default disposition first, so a second signal still kills.
	c.signalWatcher.stop()

	c.runShutdown()
}

// runShutdown starts the grace period, then cancels the Context once it ends.
func (c *Context) runShutdown() {
	close(c.shutdownTriggerC)
	timer := time.NewTimer(c.gracePeriod)
	defer timer.Stop()

	select {
	case <-c.ctx.Done():
	case <-timer.C:
		c.cancel(ErrGracePeriodExpired)
	}
}

// TriggerShutdown starts the grace period. Safe for concurrent use; later calls do nothing.
func (c *Context) TriggerShutdown() {
	c.manualShutdownTrigger()
}

// Shutdown returns a channel closed when the grace period starts.
func (c *Context) Shutdown() <-chan struct{} {
	return c.shutdownTriggerC
}

// Cancel cancels the Context immediately, skipping the grace period. Idempotent.
func (c *Context) Cancel() {
	c.cancel(ErrCanceled)
}

// ScheduledShutdown returns when shutdown will start because of the parent's
// deadline, and whether the parent has one.
func (c *Context) ScheduledShutdown() (time.Time, bool) {
	return c.scheduledShutdownTriggerTime, c.scheduledShutdownTriggerC != nil
}

// Deadline implements context.Context.
func (c *Context) Deadline() (time.Time, bool) {
	return c.ctx.Deadline()
}

// Done implements context.Context.
func (c *Context) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Err implements context.Context. Use context.Cause to tell the reasons apart.
func (c *Context) Err() error {
	return c.ctx.Err()
}

// Value implements context.Context.
func (c *Context) Value(key any) any {
	return c.ctx.Value(key)
}

// Shutdown returns the shutdown channel of ctx. It works through wrapped
// contexts, unlike a type assertion. The channel is nil when ctx is not
// graceful, so a bare receive on it blocks forever - use Shutdownable to
// check first.
func Shutdown(ctx context.Context) <-chan struct{} {
	v, _ := ctx.Value(shutdownKey{}).(<-chan struct{})
	return v
}

// Shutdownable reports whether ctx is a graceful context.
func Shutdownable(ctx context.Context) bool {
	return Shutdown(ctx) != nil
}

// Go runs fn on a new graceful Context and returns a func that triggers its
// shutdown. It panics if fn is nil. The Context is cancelled when fn returns,
// or sooner via that func or the parent.
func Go(ctx context.Context, fn func(context.Context), gracePeriod time.Duration, signals ...os.Signal) func() {
	if fn == nil {
		panic("graceful: Go called with nil function")
	}
	gctx := NewContext(ctx, gracePeriod, signals...)
	go func() {
		defer gctx.Cancel()
		fn(gctx)
	}()
	return gctx.manualShutdownTrigger
}
