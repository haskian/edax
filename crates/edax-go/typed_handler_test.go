package edax_test

import (
	"context"
	"errors"
	"testing"

	edax "github.com/haskian/edax/crates/edax-go"
)

func TestTypedHandlersValidateAtConstruction(t *testing.T) {
	codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return new(invoiceIssued) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name           string
		codec          edax.PayloadCodec
		missingHandler bool
	}{
		{"missing_codec", nil, false},
		{"missing_function", codec, true},
		{"valid", codec, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var commandFunc func(context.Context, *invoiceIssued) (string, error)
			var eventFunc func(context.Context, *invoiceIssued) error
			commandCalls, eventCalls := 0, 0
			if !test.missingHandler {
				commandFunc = func(context.Context, *invoiceIssued) (string, error) { commandCalls++; return "invoice-42", nil }
				eventFunc = func(context.Context, *invoiceIssued) error { eventCalls++; return nil }
			}
			command, commandErr := edax.TypedCommandHandler(test.codec, commandFunc)
			event, eventErr := edax.TypedEventHandler(test.codec, eventFunc)
			if test.name != "valid" {
				if command != nil || event != nil || !errors.Is(commandErr, edax.ErrBadHandle) || !errors.Is(eventErr, edax.ErrBadHandle) {
					t.Fatalf("invalid construction: command=%v event=%v", commandErr, eventErr)
				}
				return
			}
			if commandErr != nil || eventErr != nil {
				t.Fatalf("construct: %v / %v", commandErr, eventErr)
			}
			if commandCalls != 0 || eventCalls != 0 {
				t.Fatal("construction invoked handler")
			}
			data := []byte(`{"total":1250,"customer_id":"customer-17"}`)
			value, err := command(context.Background(), edax.CommandMessage{Name: "InvoiceIssued", Version: 1, Data: data})
			if err != nil || value != "invoice-42" {
				t.Fatalf("command result %v: %v", value, err)
			}
			if err := event(context.Background(), edax.EventMessage{Name: "InvoiceIssued", Version: 1, Data: data}); err != nil {
				t.Fatal(err)
			}
			if commandCalls != 1 || eventCalls != 1 {
				t.Fatalf("handler calls %d/%d", commandCalls, eventCalls)
			}
		})
	}
}
