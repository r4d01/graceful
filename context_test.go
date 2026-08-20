package graceful

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

// noSignals disables signal handling. Tests that run inside a synctest bubble
// use it so that signal.Notify never wires a runtime-owned goroutine to a
// bubbled channel.
var noSignals = []os.Signal{}

func TestTriggerShutdownStartsGracePeriodThenCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewContext(context.Background(), time.Second, noSignals...)

		synctest.Wait()
		select {
		case <-c.Shutdown():
			t.Fatal("shutdown triggered before TriggerShutdown was called")
		default:
		}

		start := time.Now()
		c.TriggerShutdown()

		<-c.Shutdown()
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("TriggerShutdown took %v, want immediate", elapsed)
		}

		select {
		case <-c.Done():
			t.Fatal("cancelled before the grace period elapsed")
		default:
		}

		<-c.Done()
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("cancelled after %v, want 1s", elapsed)
		}
		if err := c.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("Err() = %v, want context.Canceled", err)
		}
		if cause := context.Cause(c); !errors.Is(cause, ErrGracePeriodExpired) {
			t.Errorf("Cause() = %v, want ErrGracePeriodExpired", cause)
		}
	})
}

func TestTriggerShutdownIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewContext(context.Background(), time.Second, noSignals...)
		c.TriggerShutdown()
		c.TriggerShutdown()
		c.TriggerShutdown()
		<-c.Done()
	})
}

func TestCancelCancelsImmediatelyWithoutTriggeringShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewContext(context.Background(), time.Hour, noSignals...)

		start := time.Now()
		c.Cancel()
		<-c.Done()

		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Cancel took %v, want immediate", elapsed)
		}
		if cause := context.Cause(c); !errors.Is(cause, ErrCanceled) {
			t.Errorf("Cause() = %v, want ErrCanceled", cause)
		}

		synctest.Wait()
		select {
		case <-c.Shutdown():
			t.Error("Cancel triggered the grace period")
		default:
		}

		c.Cancel() // idempotent
	})
}

func TestCancelDuringGracePeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewContext(context.Background(), time.Hour, noSignals...)
		c.TriggerShutdown()
		<-c.Shutdown()

		start := time.Now()
		c.Cancel()
		<-c.Done()

		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Cancel took %v, want immediate", elapsed)
		}
		if cause := context.Cause(c); !errors.Is(cause, ErrCanceled) {
			t.Errorf("Cause() = %v, want ErrCanceled", cause)
		}
	})
}

func TestParentCancelPropagates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		c := NewContext(parent, time.Hour, noSignals...)

		cancelParent()
		<-c.Done()

		if cause := context.Cause(c); !errors.Is(cause, context.Canceled) {
			t.Errorf("Cause() = %v, want context.Canceled", cause)
		}

		synctest.Wait()
		select {
		case <-c.Shutdown():
			t.Error("a cancelled parent triggered the grace period")
		default:
		}
	})
}

func TestScheduledShutdownFromParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			timeout     = 10 * time.Second
			gracePeriod = 3 * time.Second
		)

		start := time.Now()
		parent, cancelParent := context.WithTimeout(context.Background(), timeout)
		defer cancelParent()

		c := NewContext(parent, gracePeriod, noSignals...)

		scheduled, ok := c.ScheduledShutdown()
		if !ok {
			t.Fatal("ScheduledShutdown reported no deadline")
		}
		if want := start.Add(timeout - gracePeriod); !scheduled.Equal(want) {
			t.Errorf("ScheduledShutdown() = %v, want %v", scheduled, want)
		}
		if hard, ok := c.Deadline(); !ok || !hard.Equal(start.Add(timeout)) {
			t.Errorf("Deadline() = %v, %v, want %v, true", hard, ok, start.Add(timeout))
		}

		<-c.Shutdown()
		if elapsed := time.Since(start); elapsed != timeout-gracePeriod {
			t.Errorf("shutdown triggered after %v, want %v", elapsed, timeout-gracePeriod)
		}

		<-c.Done()
		if elapsed := time.Since(start); elapsed != timeout {
			t.Errorf("cancelled after %v, want %v", elapsed, timeout)
		}
	})
}

func TestNoScheduledShutdownWithoutParentDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewContext(context.Background(), time.Second, noSignals...)
		defer c.Cancel()

		if _, ok := c.ScheduledShutdown(); ok {
			t.Error("ScheduledShutdown reported a deadline for a parent without one")
		}
		if _, ok := c.Deadline(); ok {
			t.Error("Deadline reported a deadline for a parent without one")
		}
	})
}

func TestShutdownSurvivesContextWrapping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewContext(context.Background(), time.Second, noSignals...)
		defer c.Cancel()

		type key struct{}
		var wrapped context.Context = context.WithValue(c, key{}, "v")
		wrapped, cancelWrapped := context.WithCancel(wrapped)
		defer cancelWrapped()

		ch := Shutdown(wrapped)
		if ch == nil {
			t.Fatal("Shutdown did not find the graceful context through wrappers")
		}
		if ch != c.Shutdown() {
			t.Error("Shutdown returned a different channel than the method")
		}

		c.TriggerShutdown()
		<-ch
	})
}

func TestShutdownOnPlainContext(t *testing.T) {
	if Shutdownable(context.Background()) {
		t.Error("Shutdownable reported true for a plain context")
	}
	if ch := Shutdown(context.Background()); ch != nil {
		t.Error("Shutdown returned a non-nil channel for a plain context")
	}
}

func TestValuePassesThroughToParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		parent := context.WithValue(context.Background(), key{}, "v")

		c := NewContext(parent, time.Second, noSignals...)
		defer c.Cancel()

		if got := c.Value(key{}); got != "v" {
			t.Errorf("Value() = %v, want \"v\"", got)
		}
	})
}

func TestGoRunsFunctionAndReturnsTriggerShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observed := make(chan context.Context, 1)
		triggerShutdown := Go(context.Background(), func(ctx context.Context) {
			observed <- ctx
			<-ctx.Done()
		}, time.Second, noSignals...)

		ctx := <-observed
		shutdownC := Shutdown(ctx)
		if shutdownC == nil {
			t.Fatal("Go did not pass a graceful context to fn")
		}

		triggerShutdown()
		<-shutdownC
		<-ctx.Done()
	})
}

func TestGoPanicsOnNilFunction(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Go with a nil function did not panic")
		}
	}()
	Go(context.Background(), nil, time.Second, noSignals...)
}

func TestZeroGracePeriodCancelsOnTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := NewContext(context.Background(), 0, noSignals...)

		start := time.Now()
		c.TriggerShutdown()
		<-c.Done()

		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("cancelled after %v, want immediate", elapsed)
		}
	})
}

// TestSignalTriggersShutdown runs outside a synctest bubble because signal
// delivery is driven by the runtime, not by the fake clock.
func TestSignalTriggersShutdown(t *testing.T) {
	c := NewContext(context.Background(), 10*time.Millisecond, syscall.SIGUSR1)
	defer c.Cancel()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	select {
	case <-c.Shutdown():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the signal to trigger shutdown")
	}

	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the grace period to expire")
	}
	if cause := context.Cause(c); !errors.Is(cause, ErrGracePeriodExpired) {
		t.Errorf("Cause() = %v, want ErrGracePeriodExpired", cause)
	}
}

func TestNewOsSignalWatcherSelection(t *testing.T) {
	t.Run("nil selects defaults", func(t *testing.T) {
		w := newSignalWatcher(nil)
		defer w.stop()
		if w.c == nil {
			t.Fatal("nil slice produced no channel")
		}
	})
	t.Run("empty selects none", func(t *testing.T) {
		w := newSignalWatcher([]os.Signal{})
		defer w.stop()
		if w.c != nil {
			t.Error("empty slice produced a channel; signal.Notify would relay every signal")
		}
	})
}

func TestTriggerFiresOnceAndIsSafeToRefire(t *testing.T) {
	fired, fire := newTrigger()

	select {
	case <-fired:
		t.Fatal("trigger fired before fire was called")
	default:
	}

	fire()
	fire()
	<-fired
}
