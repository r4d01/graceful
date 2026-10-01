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
		gctx, cancel := NewContext(context.Background(), time.Second, noSignals...)
		defer cancel()

		synctest.Wait()
		select {
		case <-gctx.ShutdownNotice():
			t.Fatal("shutdown triggered before TriggerShutdown was called")
		default:
		}

		start := time.Now()
		gctx.TriggerShutdown()

		<-gctx.ShutdownNotice()
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("TriggerShutdown took %v, want immediate", elapsed)
		}

		select {
		case <-gctx.Done():
			t.Fatal("cancelled before the grace period elapsed")
		default:
		}

		<-gctx.Done()
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Errorf("cancelled after %v, want 1s", elapsed)
		}
		if err := gctx.Err(); !errors.Is(err, context.Canceled) {
			t.Errorf("Err() = %v, want context.Canceled", err)
		}
		if cause := context.Cause(gctx); !errors.Is(cause, ErrGracePeriodExpired) {
			t.Errorf("Cause() = %v, want ErrGracePeriodExpired", cause)
		}
	})
}

func TestTriggerShutdownIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gctx, cancel := NewContext(context.Background(), time.Second, noSignals...)
		defer cancel()

		gctx.TriggerShutdown()
		gctx.TriggerShutdown()
		gctx.TriggerShutdown()
		<-gctx.Done()
	})
}

func TestCancelCancelsImmediatelyWithoutTriggeringShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gctx, cancel := NewContext(context.Background(), time.Hour, noSignals...)

		start := time.Now()
		cancel()
		<-gctx.Done()

		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Cancel took %v, want immediate", elapsed)
		}
		if cause := context.Cause(gctx); !errors.Is(cause, ErrCanceled) {
			t.Errorf("Cause() = %v, want ErrCanceled", cause)
		}

		synctest.Wait()
		select {
		case <-gctx.ShutdownNotice():
			t.Error("Cancel triggered the grace period")
		default:
		}

		cancel() // idempotent
	})
}

func TestCancelDuringGracePeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gctx, cancel := NewContext(context.Background(), time.Hour, noSignals...)

		gctx.TriggerShutdown()
		<-gctx.ShutdownNotice()

		start := time.Now()
		cancel()
		<-gctx.Done()

		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Cancel took %v, want immediate", elapsed)
		}
		if cause := context.Cause(gctx); !errors.Is(cause, ErrCanceled) {
			t.Errorf("Cause() = %v, want ErrCanceled", cause)
		}
	})
}

func TestParentCancelPropagates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		gctx, cancel := NewContext(parent, time.Hour, noSignals...)
		defer cancel()

		cancelParent()
		<-gctx.Done()

		if cause := context.Cause(gctx); !errors.Is(cause, context.Canceled) {
			t.Errorf("Cause() = %v, want context.Canceled", cause)
		}

		synctest.Wait()
		select {
		case <-gctx.ShutdownNotice():
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

		gctx, cancel := NewContext(parent, gracePeriod, noSignals...)
		defer cancel()

		scheduled, ok := gctx.ScheduledShutdown()
		if !ok {
			t.Fatal("ScheduledShutdown reported no deadline")
		}
		if want := start.Add(timeout - gracePeriod); !scheduled.Equal(want) {
			t.Errorf("ScheduledShutdown() = %v, want %v", scheduled, want)
		}
		if hard, ok := gctx.Deadline(); !ok || !hard.Equal(start.Add(timeout)) {
			t.Errorf("Deadline() = %v, %v, want %v, true", hard, ok, start.Add(timeout))
		}

		<-gctx.ShutdownNotice()
		if elapsed := time.Since(start); elapsed != timeout-gracePeriod {
			t.Errorf("shutdown triggered after %v, want %v", elapsed, timeout-gracePeriod)
		}

		<-gctx.Done()
		if elapsed := time.Since(start); elapsed != timeout {
			t.Errorf("cancelled after %v, want %v", elapsed, timeout)
		}
	})
}

func TestNoScheduledShutdownWithoutParentDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gctx, cancel := NewContext(context.Background(), time.Second, noSignals...)
		defer cancel()

		if _, ok := gctx.ScheduledShutdown(); ok {
			t.Error("ScheduledShutdown reported a deadline for a parent without one")
		}
		if _, ok := gctx.Deadline(); ok {
			t.Error("Deadline reported a deadline for a parent without one")
		}
	})
}

func TestShutdownNoticeSurvivesContextWrapping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gctx, cancel := NewContext(context.Background(), time.Second, noSignals...)
		defer cancel()

		type key struct{}
		var wrapped context.Context = context.WithValue(gctx, key{}, "v")
		wrapped, cancelWrapped := context.WithCancel(wrapped)
		defer cancelWrapped()

		ch := ShutdownNotice(wrapped)
		if ch == nil {
			t.Fatal("ShutdownNotice did not find the graceful context through wrappers")
		}
		if ch != gctx.ShutdownNotice() {
			t.Error("ShutdownNotice returned a different channel than the method")
		}

		gctx.TriggerShutdown()
		<-ch
	})
}

func TestShutdownNoticeOnPlainContext(t *testing.T) {
	if Shutdownable(context.Background()) {
		t.Error("Shutdownable reported true for a plain context")
	}
	if ch := ShutdownNotice(context.Background()); ch != nil {
		t.Error("ShutdownNotice returned a non-nil channel for a plain context")
	}
}

func TestValuePassesThroughToParent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type key struct{}
		parent := context.WithValue(context.Background(), key{}, "v")

		gctx, cancel := NewContext(parent, time.Second, noSignals...)
		defer cancel()

		if got := gctx.Value(key{}); got != "v" {
			t.Errorf("Value() = %v, want \"v\"", got)
		}
	})
}

func TestGoRunsFunctionAndReturnsShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observed := make(chan context.Context, 1)
		shutdown := Go(context.Background(), func(ctx context.Context) {
			observed <- ctx
			<-ctx.Done()
		}, time.Second, noSignals...)

		ctx := <-observed
		noticeC := ShutdownNotice(ctx)
		if noticeC == nil {
			t.Fatal("Go did not pass a graceful context to fn")
		}

		shutdown(false)
		<-noticeC
		<-ctx.Done()
	})
}

func TestGoShutdownWaitsForFunctionToReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		returned := false
		shutdown := Go(context.Background(), func(ctx context.Context) {
			<-ShutdownNotice(ctx)
			time.Sleep(time.Second) // wrapping up, well within the grace period
			returned = true
		}, time.Hour, noSignals...)

		shutdown(true)
		if !returned {
			t.Error("shutdown with wait true returned before fn did")
		}
	})
}

func TestGoCancelsContextWhenFunctionReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		observed := make(chan context.Context, 1)
		Go(context.Background(), func(ctx context.Context) {
			observed <- ctx
		}, time.Hour, noSignals...)

		ctx := <-observed
		<-ctx.Done()
		if cause := context.Cause(ctx); !errors.Is(cause, ErrCanceled) {
			t.Errorf("Cause() = %v, want ErrCanceled", cause)
		}

		synctest.Wait()
		select {
		case <-ShutdownNotice(ctx):
			t.Error("fn returning triggered the grace period")
		default:
		}
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

func TestNegativeGracePeriodPanics(t *testing.T) {
	t.Run("NewContext", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("NewContext with a negative grace period did not panic")
			}
		}()
		NewContext(context.Background(), -time.Second, noSignals...)
	})
	t.Run("Go", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("Go with a negative grace period did not panic")
			}
		}()
		Go(context.Background(), func(context.Context) {}, -time.Second, noSignals...)
	})
}

func TestZeroGracePeriodCancelsOnTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gctx, cancel := NewContext(context.Background(), 0, noSignals...)
		defer cancel()

		start := time.Now()
		gctx.TriggerShutdown()
		<-gctx.Done()

		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("cancelled after %v, want immediate", elapsed)
		}
	})
}

// TestSignalTriggersShutdown runs outside a synctest bubble because signal
// delivery is driven by the runtime, not by the fake clock.
func TestSignalTriggersShutdown(t *testing.T) {
	gctx, cancel := NewContext(context.Background(), 10*time.Millisecond, syscall.SIGUSR1)
	defer cancel()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	select {
	case <-gctx.ShutdownNotice():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the signal to trigger shutdown")
	}

	select {
	case <-gctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the grace period to expire")
	}
	if cause := context.Cause(gctx); !errors.Is(cause, ErrGracePeriodExpired) {
		t.Errorf("Cause() = %v, want ErrGracePeriodExpired", cause)
	}
}

func TestNewSignalWatcherSelection(t *testing.T) {
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
