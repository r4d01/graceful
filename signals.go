package graceful

import (
	"os"
	"os/signal"
)

// signalWatcher relays OS signals on c until stop is called.
type signalWatcher struct {
	c    <-chan os.Signal
	stop func()
}

// newSignalWatcher watches the given signals. A nil slice watches
// defaultSignals; a non-nil empty slice watches none - signal.Notify with no
// signals would relay everything instead.
func newSignalWatcher(signals []os.Signal) *signalWatcher {
	if signals == nil {
		signals = defaultSignals
	} else if len(signals) < 1 {
		return &signalWatcher{
			c:    nil,
			stop: func() {}}
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	return &signalWatcher{
		c:    c,
		stop: func() { signal.Stop(c) }}
}
