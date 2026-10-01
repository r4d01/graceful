package graceful

import (
	"context"
	"sync"
	"time"
)

// newTrigger returns a channel closed the first time fire is called.
func newTrigger() (fired <-chan struct{}, fire func()) {
	var once sync.Once
	ch := make(chan struct{})
	return ch, func() {
		once.Do(func() { close(ch) })
	}
}

// preDeadlineTrigger returns a timer for gracePeriod before ctx's deadline,
// or nil if ctx has no deadline.
func preDeadlineTrigger(ctx context.Context, gracePeriod time.Duration) (<-chan time.Time, time.Time) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, time.Time{}
	}

	triggerTime := deadline.Add(-gracePeriod)
	return time.After(time.Until(triggerTime)), triggerTime
}
