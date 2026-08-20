package graceful

import "sync"

// newTrigger returns a channel closed the first time fire is called.
func newTrigger() (fired <-chan struct{}, fire func()) {
	var once sync.Once
	ch := make(chan struct{})
	return ch, func() {
		once.Do(func() { close(ch) })
	}
}
