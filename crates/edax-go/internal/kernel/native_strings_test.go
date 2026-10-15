package kernel

import (
	"errors"
	"testing"
)

func TestNativeTextConversionRefusesLoss(t *testing.T) {
	for _, text := range []struct{ name, value string }{{"nul", "invoice\x00other"}, {"invalid_utf8", string([]byte{'i', 255})}} {
		t.Run(text.name, func(t *testing.T) {
			checks := []struct {
				name string
				run  func() error
			}{
				{"direct_fallible", func() error { _, err := TenantKey("silo", text.value, nil); return err }},
				{"direct_previously_infallible", func() error { _, err := Partition(text.value, 4); return err }},
				{"optional_string", func() error { _, err := OrderingKey("invoice-42", &text.value); return err }},
				{"lineage", func() error { _, err := Propagate("command-42", text.value); return err }},
				{"dto_identifier", func() error {
					_, err := EncodeCommand("json", CommandMessage{ID: text.value, Name: "PayInvoice", Version: 1, Data: []byte(`{"invoice_id":"invoice-42"}`)})
					return err
				}},
				{"dto_metadata", func() error {
					_, err := EncodeCommand("json", CommandMessage{ID: "command-42", Name: "PayInvoice", Version: 1, Metadata: map[string]string{"reference": text.value}})
					return err
				}},
				{"collection_identifier", func() error {
					database := NewMemoryTransactor()
					defer database.Close()
					store, err := database.EventStore()
					if err != nil {
						return err
					}
					defer store.Close()
					return store.MarkPublished([]string{text.value})
				}},
				{"handle_argument", func() error {
					database := NewMemoryTransactor()
					defer database.Close()
					store, err := database.ProjectionStore()
					if err != nil {
						return err
					}
					defer store.Close()
					_, err = store.Get(NoTx(), "read_model", text.value)
					return err
				}},
			}
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					if err := check.run(); !errors.Is(err, ErrCodec) {
						t.Fatalf("lossy text reached native boundary: %v", err)
					}
				})
			}
			if got := ErrorFromWire("store: " + text.value); got.Code != "codec" {
				t.Fatalf("unsafe error wire: %+v", got)
			}
			if IsTerminal(&Error{Code: ErrorCode("codec" + text.value)}) || IsFatal(&Error{Code: ErrorCode("bad_handle" + text.value)}) {
				t.Fatal("invalid error code was interpreted as a known prefix")
			}
		})
	}
}

func TestNativeTextAcceptsUnicodeAndBinary(t *testing.T) {
	id := "račun-42-☕"
	key, err := TenantKey("silo", id, nil)
	if err != nil || key != id {
		t.Fatalf("unicode key %q: %v", key, err)
	}
	_, err = Partition(id, 4)
	if err != nil {
		t.Fatal(err)
	}
	message := CommandMessage{ID: id, Name: "PayInvoice", Version: 1, Data: []byte{0, 255, 0, 254}, CorrelationID: "flow-42", CausationID: "flow-42"}
	wire, err := EncodeCommand("msgpack", message)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCommand("msgpack", wire)
	if err != nil || string(got.Data) != string(message.Data) {
		t.Fatalf("binary payload %+v: %v", got, err)
	}
}

func TestNativeConversionErrorsPreserveKernelTaxonomy(t *testing.T) {
	for _, test := range []struct {
		name, wire string
		want       error
	}{
		{"optional_json", "[2] Invalid JSON in parameter 'available': invalid type: null, expected a sequence", ErrCodec},
		{"field_serialization", "[1] failed to serialize FFI field value", ErrCodec},
		{"go_conversion", "failed to unmarshal: invalid character 'x' looking for beginning of value", ErrCodec},
		{"kernel_store", "[2] store: invoice database unavailable", ErrStore},
		{"kernel_concurrency", "[2] concurrency: invoice-42 version changed", ErrConcurrency},
		{"kernel_codec", "[2] codec: malformed invoice payload", ErrCodec},
		{"unclassified", "[2] invoice database unavailable", ErrStore},
		{"unknown_numeric_code", "[900] invoice database unavailable", ErrStore},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := errors.New(test.wire)
			got := mapNativeError(original)
			if !errors.Is(got, test.want) || !errors.Is(got, original) {
				t.Fatalf("mapped %q to %v; want %v and retained cause", test.wire, got, test.want)
			}
		})
	}
}
