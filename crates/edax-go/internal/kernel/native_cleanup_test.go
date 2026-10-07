package kernel

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countedResource struct{ frees atomic.Int32 }

func (resource *countedResource) Free() { resource.frees.Add(1) }

type cleanupOwner struct {
	state   *nativeState[*countedResource]
	cleanup runtime.Cleanup
}

func ownResource(resource *countedResource) *cleanupOwner {
	state := &nativeState[*countedResource]{inner: resource}
	owner := &cleanupOwner{state: state}
	owner.cleanup = runtime.AddCleanup(owner, (*nativeState[*countedResource]).close, state)
	return owner
}

func TestNativeCleanup(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "forgotten owner", true: "concurrent explicit close"}[explicit], func(t *testing.T) {
			resource := &countedResource{}
			func() {
				owner := ownResource(resource)
				if explicit {
					var callers sync.WaitGroup
					for range 16 {
						callers.Go(func() { owner.cleanup.Stop(); owner.state.close() })
					}
					callers.Wait()
				}
				runtime.KeepAlive(owner)
			}()
			deadline := time.Now().Add(5 * time.Second)
			for resource.frees.Load() == 0 && time.Now().Before(deadline) {
				runtime.GC()
				time.Sleep(time.Millisecond)
			}
			if got := resource.frees.Load(); got != 1 {
				t.Fatalf("Free called %d times; want exactly once", got)
			}
		})
	}
}

func TestNativeCleanupWaitsForCall(t *testing.T) {
	resource := &countedResource{}
	owner := ownResource(resource)
	owner.state.mu.Lock()
	finished := make(chan struct{})
	go func() { owner.state.close(); close(finished) }()
	if resource.frees.Load() != 0 {
		t.Fatal("resource freed during native call")
	}
	owner.state.mu.Unlock()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish after native call")
	}
	owner.cleanup.Stop()
	if resource.frees.Load() != 1 {
		t.Fatal("resource was not freed once")
	}
	runtime.KeepAlive(owner)
}
