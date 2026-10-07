package edax_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/haskian/edax/crates/edax-go"
)

// Const declarations also prove that callers cannot replace the sentinels.
const immutableCodec = edax.ErrCodec
const immutableStopped = edax.ErrDispatcherStopped

func TestImmutableSentinelCategories(t *testing.T) {
	for _, tc := range []struct {
		code     edax.ErrorCode
		sentinel error
	}{
		{edax.CodeConcurrency, edax.ErrConcurrency},
		{edax.CodeStore, edax.ErrStore},
		{edax.CodeCodec, immutableCodec},
		{edax.CodePayloadMismatch, edax.ErrPayloadMismatch},
		{edax.CodeInProgress, edax.ErrInProgress},
		{edax.CodeCachedFailure, edax.ErrCachedFailure},
		{edax.CodeHandlerNotFound, edax.ErrHandlerNotFound},
		{edax.CodeAlreadyRegistered, edax.ErrAlreadyRegistered},
		{edax.CodeBadHandle, edax.ErrBadHandle},
		{edax.CodeRejected, edax.ErrRejected},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			failure := edax.WrapError(tc.code, "invoice command failed", errors.New("original cause"))
			wrapped := fmt.Errorf("handler: %w", tc.sentinel)
			if !errors.Is(failure, tc.sentinel) || !errors.Is(wrapped, failure) {
				t.Fatal("category identity lost")
			}
			if errors.Is(failure, immutableStopped) {
				t.Fatal("SDK lifecycle error matched kernel category")
			}
			if edax.IsTerminal(wrapped) != edax.IsTerminal(failure) || edax.IsFatal(wrapped) != edax.IsFatal(failure) {
				t.Fatal("classification changed for sentinel")
			}
			var detail *edax.Error
			if errors.As(wrapped, &detail) {
				t.Fatal("sentinel exposed mutable failure details")
			}
			if !errors.As(failure, &detail) || detail.Message != "invoice command failed" {
				t.Fatal("failure details unavailable")
			}
			detail.Code = edax.CodeStore
			if !errors.Is(edax.WrapError(tc.code, "new failure", nil), tc.sentinel) {
				t.Fatal("mutating a failure changed its sentinel")
			}
		})
	}
	if !edax.IsFatal(errors.Join(edax.ErrStore, edax.ErrBadHandle)) {
		t.Fatal("joined sentinel lost fatal precedence")
	}
}
