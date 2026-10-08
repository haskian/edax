package edax

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
	"github.com/vmihailenco/msgpack/v5"
)

type shippingAddress struct {
	City     string `json:"city"`
	Postcode string `json:"postcode"`
}
type trackingToken struct{ text string }

func (t *trackingToken) DecodeMsgpack(decoder *msgpack.Decoder) error {
	text, err := decoder.DecodeString()
	if err == nil {
		t.text = text
	}
	return err
}

type shipmentPayload struct {
	OrderID      string                     `json:"order_id"`
	Address      *shippingAddress           `json:"address"`
	Previous     []shippingAddress          `json:"previous"`
	Destinations map[string]shippingAddress `json:"destinations"`
	Tags         []string                   `json:"tags"`
	Token        trackingToken              `json:"token"`
	Amount       Decimal                    `json:"amount"`
	At           Instant                    `json:"at"`
	Quantity     uint8                      `json:"quantity"`
	Delta        int8                       `json:"delta"`
}

func TestMessagePackStructShapes(t *testing.T) {
	codec, err := NewMessagePackCodec(map[string]func() any{"Shipment": func() any { return new(shipmentPayload) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(map[string]any) any
		reject bool
	}{
		{name: "maps, slices and custom scalars", change: func(value map[string]any) any { return value }},
		{name: "nullable pointer", change: func(value map[string]any) any { value["address"] = nil; return value }},
		{name: "nullable slice and map", change: func(value map[string]any) any { value["previous"], value["destinations"] = nil, nil; return value }},
		{name: "unsigned overflow", change: func(value map[string]any) any { value["quantity"] = 256; return value }, reject: true},
		{name: "negative into unsigned", change: func(value map[string]any) any { value["quantity"] = -1; return value }, reject: true},
		{name: "signed overflow", change: func(value map[string]any) any { value["delta"] = 200; return value }, reject: true},
		{name: "wide unsigned into signed", change: func(value map[string]any) any { value["delta"] = uint64(1) << 63; return value }, reject: true},
		{name: "scalar nil", change: func(value map[string]any) any { value["order_id"] = nil; return value }, reject: true},
		{name: "slice element nil", change: func(value map[string]any) any { value["previous"] = []any{nil}; return value }, reject: true},
		{name: "root positional array", change: func(map[string]any) any {
			return []any{"order-1", nil, nil, nil, nil, "tracking-1", "9.5", "2024-01-01T00:30:00.000000Z"}
		}, reject: true},
		{name: "root null", change: func(map[string]any) any { return nil }, reject: true},
		{name: "nested pointer positional array", change: func(value map[string]any) any { value["address"] = []string{"Ljubljana", "1000"}; return value }, reject: true},
		{name: "slice element positional array", change: func(value map[string]any) any { value["previous"] = []any{[]string{"Ljubljana", "1000"}}; return value }, reject: true},
		{name: "map value positional array", change: func(value map[string]any) any {
			value["destinations"] = map[string]any{"home": []string{"Ljubljana", "1000"}}
			return value
		}, reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			address := map[string]any{"city": "Ljubljana", "postcode": "1000"}
			base := map[string]any{"order_id": "order-1", "address": address, "previous": []any{address}, "destinations": map[string]any{"home": address}, "tags": []string{"insured", "express"}, "token": "tracking-1", "amount": "9.50", "at": "2024-01-01T01:30:00+01:00", "quantity": 255, "delta": -128}
			wire, err := msgpack.Marshal(test.change(base))
			if err != nil {
				t.Fatal(err)
			}
			result, err := codec.Unmarshal(Payload{Name: "Shipment", Version: 1, Data: wire})
			if test.reject {
				if !errors.Is(err, ErrCodec) {
					t.Fatalf("result=%+v error=%v", result, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			value := result.(*shipmentPayload)
			if value.OrderID != "order-1" || value.Amount.String() != "9.5" || value.At.String() != "2024-01-01T00:30:00.000000Z" || value.Token.text != "tracking-1" || value.Quantity != 255 || value.Delta != -128 || !reflect.DeepEqual(value.Tags, []string{"insured", "express"}) {
				t.Fatalf("decoded=%+v", value)
			}
			if test.name == "maps, slices and custom scalars" && (len(value.Previous) != 1 || value.Destinations["home"].City != "Ljubljana") {
				t.Fatalf("decoded=%+v", value)
			}
			if test.name == "nullable pointer" && value.Address != nil {
				t.Fatal("nullable address was not nil")
			}
		})
	}
}

type embeddedAddress struct {
	Address shippingAddress `msgpack:"destination"`
}
type embeddedShipment struct {
	embeddedAddress
	OrderID string `json:"order_id"`
}

func TestMessagePackEmbeddedAndExplicitTags(t *testing.T) {
	codec, err := NewMessagePackCodec(map[string]func() any{"Shipment": func() any { return new(embeddedShipment) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		address any
		reject  bool
	}{
		{"map", map[string]any{"city": "Ljubljana", "postcode": "1000"}, false},
		{"array", []string{"Ljubljana", "1000"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			wire, err := msgpack.Marshal(map[string]any{"order_id": "order-1", "destination": test.address})
			if err != nil {
				t.Fatal(err)
			}
			value, err := codec.Unmarshal(Payload{Name: "Shipment", Version: 1, Data: wire})
			if test.reject {
				if !errors.Is(err, ErrCodec) {
					t.Fatalf("value=%v err=%v", value, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if value.(*embeddedShipment).Address.City != "Ljubljana" {
				t.Fatal(value)
			}
		})
	}
}
func TestMessagePackLegacyTimestamp(t *testing.T) {
	type checkpoint struct {
		At Instant `json:"at"`
	}
	codec, err := NewMessagePackCodec(map[string]func() any{"Checkpoint": func() any { return new(checkpoint) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		nanos  int
		reject bool
	}{
		{"seconds", 0, false}, {"microseconds", 123000, false}, {"finer than microsecond", 123001, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stamp := time.Date(2024, 1, 1, 0, 30, 0, test.nanos, time.UTC)
			wire, err := msgpack.Marshal(map[string]any{"at": stamp})
			if err != nil {
				t.Fatal(err)
			}
			value, err := codec.Unmarshal(Payload{Name: "Checkpoint", Version: 1, Data: wire})
			if test.reject {
				if !errors.Is(err, ErrCodec) {
					t.Fatalf("value=%v err=%v", value, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			expected, err := kernel.CanonicalTimestamp(stamp.Format(time.RFC3339Nano))
			if err != nil {
				t.Fatal(err)
			}
			if value.(*checkpoint).At.String() != expected {
				t.Fatalf("value=%v expected=%v", value, expected)
			}
		})
	}
}

type positionalAddress struct {
	_msgpack struct{} `msgpack:",as_array"`
	City     string   `json:"city"`
}
type positionalHistory struct {
	OrderID  string              `json:"order_id"`
	Previous []positionalAddress `json:"previous"`
}

func TestMessagePackRefusesPositionalStructs(t *testing.T) {
	for _, test := range []struct {
		name        string
		constructor func() any
		reject      bool
	}{
		{"named fields", func() any { return new(shipmentPayload) }, false},
		{"root as_array", func() any { return new(positionalAddress) }, true},
		{"nested as_array", func() any { return new(positionalHistory) }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			codec, err := NewMessagePackCodec(map[string]func() any{"Shipment": test.constructor})
			if test.reject != errors.Is(err, ErrCodec) || test.reject != (codec == nil) {
				t.Fatalf("codec=%v err=%v", codec, err)
			}
		})
	}
}
