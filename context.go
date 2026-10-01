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

// ErrCanceled is the context.Cause of a Context cancelled by the func NewContext returns.
var ErrCanceled = errors.New("graceful: canceled")

var defaultSignals = []os.Signal{syscall.SIGTERM}

// shutdownKey carries the shutdown-notice channel, so ShutdownNotice works through wrapped contexts.
type shutdownKey struct{}

// Context is a context.Context that signals before it cancels. Create one with NewContext.
type Context struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	gracePeriod time.Duration

	shutdownNoticeC chan struct{}

	scheduledShutdownTriggerC    <-chan time.Time
	scheduledShutdownTriggerTime time.Time

	manualShutdownTriggerC <-chan struct{}
	manualShutdownTrigger  func()

	signalWatcher *signalWatcher
}

// NewContext creates a graceful context. Shutdown begins on any of the given
// signals, or gracePeriod before the parent's deadline. The Context is
// cancelled gracePeriod after shutdown begins.
//
// Passing no signals watches SIGTERM; passing an empty non-nil slice watches none.
// It panics if gracePeriod is negative.
//
// The Context and its goroutine live until it is cancelled: by the returned
// func, by the grace period, or by the parent. That func cancels immediately,
// skipping the grace period, and is idempotent - defer it the way you would a
// context.CancelFunc.
func NewContext(parent context.Context, gracePeriod time.Duration, signals ...os.Signal) (*Context, func()) {
	if gracePeriod < 0 {
		panic("graceful: negative grace period")
	}

	shutdownNoticeC := make(chan struct{})

	// A timer, unlike a signal, cannot be missed by starting late.
	scheduledShutdownTriggerC, scheduledShutdownTriggerTime := preDeadlineTrigger(parent, gracePeriod)

	manualShutdownTriggerC, manualShutdownTrigger := newTrigger()

	// Registered here, not in the goroutine: a signal arriving before the
	// goroutine runs would be missed, and would kill the process.
	signalWatcher := newSignalWatcher(signals)

	ctx, cancel := context.WithCancelCause(parent)
	ctx = context.WithValue(ctx, shutdownKey{}, (<-chan struct{})(shutdownNoticeC))

	gctx := &Context{
		ctx:    ctx,
		cancel: cancel,

		gracePeriod: gracePeriod,

		shutdownNoticeC: shutdownNoticeC,

		scheduledShutdownTriggerC:    scheduledShutdownTriggerC,
		scheduledShutdownTriggerTime: scheduledShutdownTriggerTime,

		manualShutdownTriggerC: manualShutdownTriggerC,
		manualShutdownTrigger:  manualShutdownTrigger,

		signalWatcher: signalWatcher}
	go gctx.awaitShutdownStart()
	return gctx, func() { cancel(ErrCanceled) }
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
	// The parent can cancel between awaitShutdownStart's select and
	// this close, so the notice may arrive with no grace left.
	close(c.shutdownNoticeC)
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

// ShutdownNotice returns a channel closed when the grace period starts.
func (c *Context) ShutdownNotice() <-chan struct{} {
	return c.shutdownNoticeC
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

// ShutdownNotice returns the shutdown-notice channel of ctx. It works
// through wrapped contexts, unlike a type assertion. The channel is nil
// when ctx is not graceful: inert in a select alongside ctx.Done(), but a
// bare receive on it blocks forever - use Shutdownable to check first.
func ShutdownNotice(ctx context.Context) <-chan struct{} {
	v, _ := ctx.Value(shutdownKey{}).(<-chan struct{})
	return v
}

// Shutdownable reports whether ctx is a graceful context.
func Shutdownable(ctx context.Context) bool {
	return ShutdownNotice(ctx) != nil
}

// Go runs fn on a new graceful Context and returns a func that triggers its
// shutdown, waiting for fn to return and the Context to be cancelled if wait
// is true. Calling it with wait true from within fn deadlocks. It panics if fn
// is nil or gracePeriod is negative. The Context is cancelled when fn returns,
// or sooner via that func or the parent.
func Go(ctx context.Context, fn func(context.Context), gracePeriod time.Duration, signals ...os.Signal) func(wait bool) {
	if fn == nil {
		panic("graceful: Go called with nil function")
	}
	gctx, cancel := NewContext(ctx, gracePeriod, signals...)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		fn(gctx)
	}()

	return func(wait bool) {
		gctx.TriggerShutdown()
		if wait {
			<-done
		}
	}
}
