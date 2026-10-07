package kernel

import (
	"errors"
	"fmt"
	"testing"
)

func TestJoinedErrorClassification(t *testing.T) {
	transient := &Error{Code: CodeStore, Message: "offset commit unavailable"}
	terminal := &Error{Code: CodeCodec, Message: "invalid invoice payload"}
	fatal := &Error{Code: CodeBadHandle, Message: "consumer authorization revoked"}
	for _, tc := range []struct {
		name            string
		err             error
		code            string
		terminal, fatal bool
	}{
		{"transient then fatal", errors.Join(transient, fatal), "bad_handle", true, true},
		{"fatal then transient", errors.Join(fatal, transient), "bad_handle", true, true},
		{"wrapped nested fatal", fmt.Errorf("consume failed: %w", errors.Join(transient, errors.Join(terminal, fatal))), "bad_handle", true, true},
		{"terminal then transient", errors.Join(terminal, transient), "codec", true, false},
		{"transient then terminal", errors.Join(transient, terminal), "codec", true, false},
		{"unclassified then transient", errors.Join(errors.New("connection reset"), transient), "store", false, false},
		{"explicit outer code", &Error{Code: CodeStore, cause: errors.Join(terminal, fatal)}, "store", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code := CodeOf(tc.err)
			if code == nil || *code != tc.code || IsTerminal(tc.err) != tc.terminal || IsFatal(tc.err) != tc.fatal {
				t.Fatalf("code=%v terminal=%v fatal=%v", code, IsTerminal(tc.err), IsFatal(tc.err))
			}
			if !errors.Is(tc.err, transient) && !errors.Is(tc.err, terminal) && !errors.Is(tc.err, fatal) {
				t.Fatal("original error tree was lost")
			}
		})
	}
}
