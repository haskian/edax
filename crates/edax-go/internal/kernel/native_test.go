package kernel

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestErrorWireGolden(t *testing.T) {
	raw, err := os.ReadFile("../../../../testdata/golden/error_wire.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		Name     string
		Wire     string
		Code     string
		Message  string
		Terminal bool
	}
	if err = json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		t.Run(row.Name, func(t *testing.T) {
			got := ErrorFromWire(row.Wire)
			if string(got.Code) != row.Code || got.Message != row.Message || IsTerminal(got) != row.Terminal {
				t.Fatalf("got %+v terminal=%v; want %+v", got, IsTerminal(got), row)
			}
		})
	}
}

func TestNativeErrorAndBinaryMessage(t *testing.T) {
	err := CheckTenant("")
	if !errors.Is(err, ErrCodec) {
		t.Fatalf("typed native error: %v", err)
	}
	var typed *Error
	if !errors.As(fmt.Errorf("wrapped: %w", err), &typed) || typed.Code != "codec" {
		t.Fatalf("errors.As: %v", err)
	}
	message := EventMessage{ID: "evt-invoice-issued", AggregateID: "invoice-42", Name: "InvoiceIssued", Version: 1, Sequence: ptr(uint64(1)), Data: []byte{0, 1, 255, 0, 3}, CorrelationID: "flow-42", CausationID: "cmd-42", RecordedAt: 123, Extensions: map[string]string{}, Metadata: map[string]string{}}
	for _, format := range []string{"json", "msgpack"} {
		t.Run(format, func(t *testing.T) {
			raw, err := EncodeEvent(format, message)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecodeEvent(format, raw)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(*got, message) {
				t.Fatalf("roundtrip %+v, want %+v", got, message)
			}
		})
	}
}

func TestNativeHandleClose(t *testing.T) {
	database := GetMemoryTransactor()
	events, err := database.EventStore()
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = database.Begin(); !errors.Is(err, ErrBadHandle) {
		t.Fatalf("closed begin: %v", err)
	}
	// A store owns an Arc of the database; closing the factory does not close it.
	if got, err := events.Load(NoTx(), "invoice-42", 0); err != nil || len(got) != 0 {
		t.Fatalf("retained store: %v %v", got, err)
	}
	var workers sync.WaitGroup
	for range 20 {
		workers.Go(func() {
			for range 20 {
				_, err := events.Load(NoTx(), "invoice-42", 0)
				if err != nil && !errors.Is(err, ErrBadHandle) {
					t.Errorf("concurrent close: %v", err)
				}
			}
		})
	}
	if err = events.Close(); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	if err = events.Close(); err != nil {
		t.Fatal(err)
	}
}

// Alef 0.106.1 omits Free on optional DTO method results. The public methods
// must call the ownership-safe ABI records so polling does not leak each row.
func TestOptionalNativeResultsAreOwned(t *testing.T) {
	source, err := os.ReadFile("native.gen.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ receiver, method, safe string }{
		{"MemoryEventStore", "LoadSnapshot", "LoadSnapshotResult"},
		{"MemoryProjectionStore", "Get", "GetResult"},
		{"MemorySagaStore", "Load", "LoadResult"},
		{"MemoryBroker", "TryPoll", "TryPollResult"},
		{"Drain", "Sunk", "SunkResult"},
		{"OffsetTracker", "Revoke", "RevokeResult"},
	} {
		t.Run(test.receiver+"."+test.method, func(t *testing.T) {
			signature := "func (h *" + test.receiver + ") " + test.method + "("
			start := strings.Index(string(source), signature)
			if start < 0 {
				t.Fatalf("missing public method %s", signature)
			}
			body := string(source[start:])
			end := strings.Index(body, "\n}\n")
			if end < 0 {
				t.Fatal("unterminated method")
			}
			body = body[:end]
			if !strings.Contains(body, "h.inner."+test.safe+"(") || strings.Contains(body, "h.inner."+test.method+"(") {
				t.Fatalf("method does not own optional native result:\n%s", body)
			}
		})
	}
}

func ptr[T any](value T) *T { return &value }
