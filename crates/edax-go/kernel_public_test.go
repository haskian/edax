package edax_test

import (
	"reflect"
	"slices"
	"testing"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

func TestPublicKernelNamesMatchTaxonomy(t *testing.T) {
	codes := []edax.ErrorCode{edax.CodeConcurrency, edax.CodeCodec, edax.CodeStore, edax.CodePayloadMismatch, edax.CodeInProgress, edax.CodeCachedFailure, edax.CodeHandlerNotFound, edax.CodeAlreadyRegistered, edax.CodeBadHandle, edax.CodeRejected, edax.CodeShredded, edax.CodeKeyMissing, edax.CodeKeyUnauthorized, edax.CodeKeyUnavailable, edax.CodeKeyRevoking, edax.CodeOwnerMismatch}
	codeNames := make([]string, len(codes))
	for i, code := range codes {
		codeNames[i] = string(code)
	}
	modes := []string{string(edax.TenancySilo), string(edax.TenancyPool), string(edax.TenancyBridge)}
	for _, test := range []struct {
		name           string
		public, kernel []string
	}{
		{"error codes", codeNames, kernel.ErrorCodes()},
		{"tenancy modes", modes, kernel.TenancyModes()},
		{"key statuses", []string{"unknown", "active", "revoking", "destroyed"}, kernel.KeyStatuses()},
		{"erasure stages", []string{"persisted", "revoked", "notified", "destruction_confirmed", "complete"}, kernel.ErasureStages()},
		{"erasure actions", []string{"revoke", "notify", "acknowledge", "destroy", "complete"}, kernel.ErasureActions()},
		{"protected replay actions", []string{"pending", "done", "failed"}, kernel.ProtectedReplayActions()},
	} {
		t.Run(test.name, func(t *testing.T) {
			slices.Sort(test.public)
			slices.Sort(test.kernel)
			if !reflect.DeepEqual(test.public, test.kernel) {
				t.Fatalf("public=%v kernel=%v", test.public, test.kernel)
			}
		})
	}
}
