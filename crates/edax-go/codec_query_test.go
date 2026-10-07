package edax_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/vmihailenco/msgpack/v5"
)

type accountOpened struct {
	Owner    string `json:"owner"`
	Currency string `json:"currency"`
}

func TestCodecUpcast(t *testing.T) {
	codec, err := edax.NewJSONCodec(map[string]func() any{"AccountOpened": func() any { return new(accountOpened) }})
	if err != nil {
		t.Fatal(err)
	}
	if err = codec.WithUpcaster("AccountOpened", &edax.Upcaster{Steps: map[uint32]func([]byte) ([]byte, error){1: func(data []byte) ([]byte, error) {
		var row map[string]any
		if err := json.Unmarshal(data, &row); err != nil {
			return nil, err
		}
		row["currency"] = "EUR"
		return json.Marshal(row)
	}}}); err != nil {
		t.Fatal(err)
	}
	got, err := codec.Unmarshal(edax.Payload{Name: "AccountOpened", Version: 1, Data: []byte(`{"owner":"Ada"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if value := got.(*accountOpened); value.Owner != "Ada" || value.Currency != "EUR" {
		t.Fatalf("%+v", value)
	}
	payload, err := codec.Marshal(accountOpened{Owner: "Ada", Currency: "EUR"})
	if err != nil || payload.Version != 2 {
		t.Fatalf("%+v %v", payload, err)
	}
	_, err = codec.Unmarshal(edax.Payload{Name: "AccountOpened", Version: 3, Data: payload.Data})
	if !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("future version accepted: %v", err)
	}
}

func TestQueryAndDecimal(t *testing.T) {
	price, err := edax.NewDecimal("10.000")
	if err != nil {
		t.Fatal(err)
	}
	documents := []edax.Document{{"id": "invoice-1", "amount": "9.50"}, {"id": "invoice-2", "amount": "12.00"}, {"id": "invoice-3", "amount": "11"}}
	query := edax.QuerySpec{Filter: edax.Greater("amount", price), Sort: []edax.SortKey{{Path: "amount", Collation: "decimal"}}, Limit: edax.Ptr(uint64(1))}
	first, err := query.Apply(documents)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Rows) != 1 || first.Rows[0]["id"] != "invoice-3" || first.Next == nil {
		t.Fatalf("%+v", first)
	}
	query.After = first.Next
	second, err := query.Apply(documents)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Rows) != 1 || second.Rows[0]["id"] != "invoice-2" || second.Next != nil {
		t.Fatalf("%+v", second)
	}
	for _, test := range []struct {
		name   string
		filter edax.Filter
		want   bool
	}{{"missing-ne", edax.NotEqual("status", "paid"), false}, {"missing-not", edax.Not(edax.Equal("status", "paid")), true}, {"empty-and", edax.And(), true}, {"empty-or", edax.Or(), false}} {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.filter.Matches(documents[0])
			if err != nil || got != test.want {
				t.Fatalf("%v %v", got, err)
			}
		})
	}
}

type pricedInvoice struct {
	Account    string       `json:"account"`
	Amount     edax.Decimal `json:"amount"`
	At         edax.Instant `json:"at"`
	Attachment []byte       `json:"attachment"`
}

func TestMessagePackPayloadAndHostKeys(t *testing.T) {
	codec, err := edax.NewMessagePackCodec(map[string]func() any{"PricedInvoice": func() any { return &pricedInvoice{} }})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := edax.NewDecimal("9.50")
	at, _ := edax.NewInstant("2024-01-01T01:30:00+01:00")
	original := &pricedInvoice{Account: "account-42", Amount: amount, At: at, Attachment: []byte{0, 255, 0, 1}}
	payload, err := codec.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Unmarshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf("roundtrip %+v want %+v", decoded, original)
	}
	// Python msgspec writes Decimal and datetime as canonical text. The binary
	// attachment stays MessagePack binary and must not become a JSON base64 string.
	var raw map[string]any
	if err = msgpack.Unmarshal(payload.Data, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["amount"] != "9.5" || raw["at"] != at.String() || !reflect.DeepEqual(raw["attachment"], original.Attachment) {
		t.Fatalf("wire %#v", raw)
	}
	payload.Data = append(payload.Data, 0)
	if _, err = codec.Unmarshal(payload); !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("trailing bytes accepted: %v", err)
	}
	keys := map[edax.Decimal]map[edax.Instant]string{amount: {at: "invoice-42"}}
	wire, err := json.Marshal(keys)
	if err != nil {
		t.Fatal(err)
	}
	var got map[edax.Decimal]map[edax.Instant]string
	if err = json.Unmarshal(wire, &got); err != nil || !reflect.DeepEqual(got, keys) {
		t.Fatalf("host keys: %s %#v %v", wire, got, err)
	}
}

func TestCodecConstructorAndNilFailures(t *testing.T) {
	changing := false
	codec, err := edax.NewJSONCodec(map[string]func() any{"AccountOpened": func() any {
		if changing {
			return nil
		}
		return &accountOpened{}
	}})
	if err != nil {
		t.Fatal(err)
	}
	changing = true
	if _, err = codec.Unmarshal(edax.Payload{Name: "AccountOpened", Version: 1, Data: []byte(`{"owner":"Ada","currency":"EUR"}`)}); !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("nil constructor: %v", err)
	}
	if _, err = codec.Marshal((*accountOpened)(nil)); !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("nil payload: %v", err)
	}
	if err = codec.WithUpcaster("AccountOpened", &edax.Upcaster{Steps: map[uint32]func([]byte) ([]byte, error){1: nil}}); !errors.Is(err, edax.ErrBadHandle) {
		t.Fatalf("nil transform: %v", err)
	}
}
