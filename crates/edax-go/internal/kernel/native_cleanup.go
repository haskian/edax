package kernel

import "sync"

// Keep cleanup state separate from its Go owner: a cleanup argument that points
// back to the owner would keep it reachable forever. The same lock serializes
// native calls, explicit Close, and the runtime's fallback cleanup.
type nativeState[T interface {
	comparable
	Free()
}] struct {
	mu    sync.Mutex
	inner T
}

func (state *nativeState[T]) close() {
	state.mu.Lock()
	defer state.mu.Unlock()
	var zero T
	if state.inner != zero {
		state.inner.Free()
		state.inner = zero
	}
}
