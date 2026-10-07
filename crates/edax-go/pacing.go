package edax

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Pacing supplies the host's sleep and entropy. The kernel computes the wait.
// The zero value uses the kernel's default policy and a cancellable timer.
type Pacing struct {
	Policy  *BackoffPolicy
	Sleep   func(context.Context, time.Duration) error
	Entropy func() uint64
}

// Wait applies kernel backoff for step and waits with ctx cancellation. Custom Sleep must honor ctx.
func (p Pacing) Wait(ctx context.Context, step uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	policy := p.Policy
	if policy == nil {
		policy = DefaultBackoff()
	}
	draw := p.Entropy
	if draw == nil {
		draw = rand.Uint64
	}
	millis := kernel.BackoffMillis(policy.BaseMillis, policy.CapMillis, step, draw())
	// time.Duration cannot represent the full kernel u64 millisecond range.
	if millis > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return &Error{Code: CodeBadHandle, Message: "backoff exceeds time.Duration"}
	}
	duration := time.Duration(millis) * time.Millisecond
	if p.Sleep != nil {
		return p.Sleep(ctx, duration)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
