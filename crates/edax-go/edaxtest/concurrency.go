package edaxtest

import (
	"context"
	"errors"
	"testing"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
)

// blockedWait is how long a race waits for the second unit of work's write before it decides the
// write is parked on the first one's row lock and commits the first. A non-blocking store answers at
// once, so only a store that parks the second writer makes the race wait this long.
const blockedWait = 200 * time.Millisecond

// outcome is how one raced unit of work ended.
type outcome string

const (
	landed   outcome = "landed"
	conflict outcome = "conflict"
)

// race runs two units of work that each stage their write before either commits, and reports how each
// ended. The order is forced: the first stages, then the second stages in a goroutine of its own (so a
// store that parks it on the first one's row lock cannot deadlock the clause), then the first commits,
// then the second does. ErrConcurrency from a write or a commit is a conflict; any other error fails.
func race(t testing.TB, owner edax.UnitOfWork, first, second func(context.Context) error) [2]outcome {
	t.Helper()
	base := ctx(t)
	staged, mayCommit, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var secondErr error
	runSecond := func() {
		defer close(ended)
		secondErr = edax.WithinTransaction(base, owner, func(txctx context.Context) error {
			if err := second(txctx); err != nil {
				return err
			}
			close(staged)
			<-mayCommit
			return nil
		})
	}
	started := false
	firstErr := edax.WithinTransaction(base, owner, func(txctx context.Context) error {
		if err := first(txctx); err != nil {
			return err
		}
		started = true
		go runSecond()
		select {
		case <-staged:
		case <-ended:
		case <-time.After(blockedWait):
		}
		return nil
	})
	close(mayCommit)
	if !started {
		t.Fatalf("the first unit of work failed before the second started: %v", firstErr)
	}
	select {
	case <-ended:
	case <-time.After(5 * time.Second):
		t.Fatal("the second unit of work never finished")
	}
	return [2]outcome{settled(t, firstErr), settled(t, secondErr)}
}

func settled(t testing.TB, err error) outcome {
	t.Helper()
	switch {
	case err == nil:
		return landed
	case errors.Is(err, edax.ErrConcurrency):
		return conflict
	}
	t.Fatalf("a raced unit of work failed for another reason: %v", err)
	return ""
}

// landedOnce fails unless exactly one of the raced writers landed, and returns the winner's name.
func landedOnce(t testing.TB, outcomes [2]outcome, firstName, secondName string) string {
	t.Helper()
	switch outcomes {
	case [2]outcome{landed, conflict}:
		return firstName
	case [2]outcome{conflict, landed}:
		return secondName
	}
	t.Fatalf("exactly one writer lands, got %v", outcomes)
	return ""
}
