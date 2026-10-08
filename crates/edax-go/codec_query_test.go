package edax_test

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

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

func TestUpcastTransformFailuresAndInPlaceEdits(t *testing.T) {
	for _, test := range []struct {
		name      string
		transform func([]byte) ([]byte, error)
		code      error
		terminal  bool
	}{
		{name: "uncoded failure is a terminal codec error", transform: func(data []byte) ([]byte, error) {
			return nil, json.Unmarshal(data, new(int))
		}, code: edax.ErrCodec, terminal: true},
		{name: "coded failure keeps its code", transform: func([]byte) ([]byte, error) {
			return nil, &edax.Error{Code: edax.CodeStore, Message: "currency service is down"}
		}, code: edax.ErrStore, terminal: false},
		{name: "in-place edit leaves the payload unchanged", transform: func(data []byte) ([]byte, error) {
			copy(data[len(data)-6:], `"GBP"`)
			return data, nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			codec, err := edax.NewJSONCodec(map[string]func() any{"AccountOpened": func() any { return new(accountOpened) }})
			if err != nil {
				t.Fatal(err)
			}
			if err = codec.WithUpcaster("AccountOpened", &edax.Upcaster{Steps: map[uint32]func([]byte) ([]byte, error){1: test.transform}}); err != nil {
				t.Fatal(err)
			}
			original := `{"owner":"Ada","currency":"EUR"}`
			payload := edax.Payload{Name: "AccountOpened", Version: 1, Data: []byte(original)}
			for range 2 {
				value, err := codec.Unmarshal(payload)
				if test.code != nil {
					if !errors.Is(err, test.code) || edax.IsTerminal(err) != test.terminal {
						t.Fatalf("value=%v err=%v terminal=%v", value, err, edax.IsTerminal(err))
					}
					return
				}
				if err != nil || value.(*accountOpened).Currency != "GBP" || string(payload.Data) != original {
					t.Fatalf("value=%+v err=%v payload=%s", value, err, payload.Data)
				}
			}
		})
	}
}

type parcelAddress struct {
	City string `json:"city"`
}
type parcelBase struct {
	Carrier string `json:"carrier"`
}
type parcelShipped struct {
	parcelBase
	OrderID  string            `json:"order_id"`
	Weight   float64           `json:"weight"`
	Address  parcelAddress     `json:"address"`
	Previous []parcelAddress   `json:"previous"`
	Labels   map[string]string `json:"labels"`
	Note     *string           `json:"note"`
	Amount   edax.Decimal      `json:"amount"`
}

func TestJSONCodecNullShapes(t *testing.T) {
	codec, err := edax.NewJSONCodec(map[string]func() any{"ParcelShipped": func() any { return new(parcelShipped) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(map[string]any) any
		reject bool
	}{
		{name: "full map", change: func(value map[string]any) any { return value }},
		{name: "nullable pointer, slice and map", change: func(value map[string]any) any {
			value["note"], value["previous"], value["labels"] = nil, nil, nil
			return value
		}},
		{name: "unknown key null", change: func(value map[string]any) any { value["extra"] = nil; return value }},
		{name: "root null", change: func(map[string]any) any { return nil }, reject: true},
		{name: "nested struct null", change: func(value map[string]any) any { value["address"] = nil; return value }, reject: true},
		{name: "nested struct null under a folded key", change: func(value map[string]any) any {
			delete(value, "address")
			value["ADDRESS"] = nil
			return value
		}, reject: true},
		{name: "slice element null", change: func(value map[string]any) any { value["previous"] = []any{nil}; return value }, reject: true},
		// A NaN or infinite weight reaches the wire as null, as in every SDK; it must not read as 0.
		{name: "float null", change: func(value map[string]any) any { value["weight"] = nil; return value }, reject: true},
		{name: "embedded field null", change: func(value map[string]any) any { value["carrier"] = nil; return value }, reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			address := map[string]any{"city": "Ljubljana"}
			base := map[string]any{"carrier": "post", "order_id": "order-1", "weight": 2.5, "address": address,
				"previous": []any{address}, "labels": map[string]any{"fragile": "yes"}, "note": "leave at door", "amount": "9.50"}
			wire, err := json.Marshal(test.change(base))
			if err != nil {
				t.Fatal(err)
			}
			value, err := codec.Unmarshal(edax.Payload{Name: "ParcelShipped", Version: 1, Data: wire})
			if test.reject {
				if !errors.Is(err, edax.ErrCodec) {
					t.Fatalf("value=%+v err=%v", value, err)
				}
				return
			}
			parcel, _ := value.(*parcelShipped)
			if err != nil || parcel.OrderID != "order-1" || parcel.Carrier != "post" || parcel.Address.City != "Ljubljana" || parcel.Amount.String() != "9.5" {
				t.Fatalf("value=%+v err=%v", value, err)
			}
		})
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
	nine5, err := edax.NewDecimal("9.5")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		filter edax.Filter
		want   bool
		refuse bool
	}{
		{name: "missing-ne", filter: edax.NotEqual("status", "paid"), want: false},
		{name: "missing-not", filter: edax.Not(edax.Equal("status", "paid")), want: true},
		{name: "empty-and", filter: edax.And(), want: true},
		{name: "empty-or", filter: edax.Or(), want: false},
		// Text order puts "9.50" after "10" and apart from "9.5"; only decimal collation matches.
		{name: "decimal-pointer-less", filter: edax.Less("amount", &price), want: true},
		{name: "decimal-pointer-equal", filter: edax.Equal("amount", &nine5), want: true},
		{name: "decimal-pointer-in", filter: edax.In("amount", "12", &nine5), want: true},
		{name: "nil-decimal-pointer", filter: edax.Equal("amount", (*edax.Decimal)(nil)), refuse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.filter.Matches(documents[0])
			if test.refuse {
				if !errors.Is(err, edax.ErrCodec) {
					t.Fatalf("%v %v", got, err)
				}
				return
			}
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

type floatInner struct {
	City   string  `json:"city,omitempty"`
	Weight float64 `json:"weight"`
	Count  int     `json:"count,string"`
}
type floatBase struct {
	Carrier string  `json:"carrier"`
	Tare    float64 `json:"tare"`
}
type floatParcel struct {
	floatBase
	*floatInner
	Gross   float64            `json:"gross,omitempty"`
	Net     float32            `json:"net"`
	Price   *float64           `json:"price,omitempty"`
	Quoted  float64            `json:"quoted,string"`
	Labels  map[string]float64 `json:"labels"`
	Skipped float64            `json:"-"`
	hidden  float64
}

// The registered payload holds the value under test, so every case runs through the codec.
type hostValue struct {
	V any `json:"v"`
}

func TestJSONCodecSpellsFloatsAsFloats(t *testing.T) {
	codec, err := edax.NewJSONCodec(map[string]func() any{"HostValue": func() any { return new(hostValue) }})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := edax.NewDecimal("9.50")
	big := 9007199254740992.0
	for _, test := range []struct {
		name  string
		value any
		want  string
	}{
		{"whole float below 2^53 keeps a fraction", 2.0, "2.0"},
		{"whole float at 2^53 keeps a fraction", big, "9007199254740992.0"},
		{"whole float at 1e20 keeps a fraction", 1e20, "100000000000000000000.0"},
		{"float at 1e21 keeps its exponent", 1e21, "1e+21"},
		{"fractional float is unchanged", 1.5, "1.5"},
		{"negative zero keeps its sign", math.Copysign(0, -1), "-0.0"},
		{"float32 keeps its own shortest spelling", []float32{3, 0.1}, "[3.0,0.1]"},
		{"NaN is null", math.NaN(), "null"},
		{"infinity is null", math.Inf(1), "null"},
		{"negative infinity is null", []any{math.Inf(-1), 1}, "[null,1]"},
		{"integer stays an integer", int64(9007199254740993), "9007199254740993"},
		{"nested struct honours tags, omitempty and embedding",
			floatParcel{floatBase: floatBase{"post", 2}, floatInner: &floatInner{Weight: 3, Count: 4}, Net: 1, Quoted: 5, Labels: map[string]float64{"b": 2, "a": 0.5}, Skipped: 6, hidden: 7},
			`{"carrier":"post","tare":2.0,"weight":3.0,"count":"4","net":1.0,"quoted":"5","labels":{"a":0.5,"b":2.0}}`},
		{"nil embedded pointer and set pointer", floatParcel{Gross: -1, Price: &big},
			`{"carrier":"","tare":0.0,"gross":-1.0,"net":0.0,"price":9007199254740992.0,"quoted":"0","labels":null}`},
		{"map of any", map[string]any{"float": 2.0, "integer": 2, "nested": []any{map[string]any{"x": 4.0}}}, `{"float":2.0,"integer":2,"nested":[{"x":4.0}]}`},
		{"json.Marshaler passes through", []any{amount, json.RawMessage(`2`)}, `["9.5",2]`},
		{"json.Number passes through", json.Number("2"), "2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := codec.Marshal(&hostValue{test.value})
			if want := `{"v":` + test.want + `}`; err != nil || string(payload.Data) != want {
				t.Fatalf("got %s %v want %s", payload.Data, err, want)
			}
		})
	}
}

// Every value here is float-free, so the codec must write exactly what encoding/json writes.
func TestJSONCodecWritesFloatFreeValuesAsEncodingJSON(t *testing.T) {
	type inner struct {
		City string `json:"city,omitempty"`
		N    int    `json:"n,string"`
	}
	type base struct {
		Shared string
		Deep   string `json:"deep"`
	}
	type other struct{ Shared string }
	type outer struct {
		base
		*other
		Inner  inner                   `json:"inner"`
		Ptr    *inner                  `json:"ptr,omitempty"`
		When   time.Time               `json:"when"`
		Zero   time.Time               `json:"zero,omitzero"`
		Bytes  []byte                  `json:"bytes"`
		Map    map[int]string          `json:"map"`
		Raw    json.RawMessage         `json:"raw"`
		Skip   string                  `json:"-"`
		Dash   string                  `json:"-,"`
		Text   string                  `json:"text,string"`
		HTML   string                  `json:"<html>"`
		Any    any                     `json:"any"`
		Nested map[string][]inner      `json:"nested"`
		Empty  map[string]bool         `json:"empty,omitempty"`
		Zeroed *time.Time              `json:"zeroed,omitzero"`
		Lists  [2][]map[string]float32 `json:"lists"`
	}
	codec, err := edax.NewJSONCodec(map[string]func() any{"HostValue": func() any { return new(hostValue) }})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{
		nil, "a<b>&", uint64(math.MaxUint64), true, []int{1, 2}, [2]string{"x", "y"}, []byte{1, 2},
		outer{base: base{"s", "d"}, Inner: inner{"c", 5}, When: time.Unix(5, 0).UTC(), Bytes: []byte("x"),
			Map: map[int]string{2: "b", 1: "a"}, Raw: json.RawMessage(`{"z":1}`), Skip: "no", Dash: "yes",
			Text: "t", HTML: "<", Any: map[string]any{"k": []int{1}}, Nested: map[string][]inner{"k": {{N: 1}}}},
		&outer{other: &other{"o"}, Ptr: &inner{}},
		edax.Document{"id": "x", "rows": []edax.Document{{"a": json.Number("1")}}},
	} {
		want, err := json.Marshal(hostValue{value})
		if err != nil {
			t.Fatal(err)
		}
		if payload, err := codec.Marshal(&hostValue{value}); err != nil || string(payload.Data) != string(want) {
			t.Errorf("%#v\n got %s %v\nwant %s", value, payload.Data, err, want)
		}
	}
}

func TestJSONCodecHonoursInvalidTagFallback(t *testing.T) {
	type shipment struct {
		OrderID string `json:"order\\id"`
		Weight  int    `json:"weight\"kg,string"`
		Carrier string `json:"運送会社"`
	}
	codec, err := edax.NewJSONCodec(map[string]func() any{"Shipment": func() any { return new(shipment) }})
	if err != nil {
		t.Fatal(err)
	}
	value := &shipment{OrderID: "order-42", Weight: 12, Carrier: "post"}
	want, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := codec.Marshal(value)
	if err != nil || string(payload.Data) != string(want) {
		t.Fatalf("encoded %s: %v, want %s", payload.Data, err, want)
	}
	decoded, err := codec.Unmarshal(payload)
	if err != nil || !reflect.DeepEqual(decoded, value) {
		t.Fatalf("decoded %+v: %v, want %+v", decoded, err, value)
	}
	for _, test := range []struct {
		name, wire string
		refuse     bool
	}{
		{"fallback name refuses null", `{"OrderID":null,"Weight":"12","運送会社":"post"}`, true},
		{"invalid tag stays unknown", `{"OrderID":"order-42","Weight":"12","運送会社":"post","order\\id":null}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := codec.Unmarshal(edax.Payload{Name: "Shipment", Version: 1, Data: []byte(test.wire)})
			if test.refuse {
				if !errors.Is(err, edax.ErrCodec) {
					t.Fatalf("decoded %+v: %v", got, err)
				}
			} else if err != nil || !reflect.DeepEqual(got, value) {
				t.Fatalf("decoded %+v: %v, want %+v", got, err, value)
			}
		})
	}
}

func TestJSONCodecLeavesNumberRangeToTheTarget(t *testing.T) {
	type numberReading struct {
		Sensor string      `json:"sensor"`
		Value  json.Number `json:"value"`
	}
	type rawReading struct {
		Sensor string          `json:"sensor"`
		Value  json.RawMessage `json:"value"`
	}
	type floatReading struct {
		Sensor string  `json:"sensor"`
		Value  float64 `json:"value"`
	}
	for _, test := range []struct {
		name        string
		constructor func() any
		wire        string
		refuse      bool
	}{
		{"number keeps large exponent", func() any { return new(numberReading) }, `{"sensor":"pressure-42","value":1e1000}`, false},
		{"raw decoder keeps large exponent", func() any { return new(rawReading) }, `{"sensor":"pressure-42","value":1e1000}`, false},
		{"unknown number is skipped", func() any { return new(floatReading) }, `{"sensor":"pressure-42","value":12.5,"extra":1e1000}`, false},
		{"float still refuses overflow", func() any { return new(floatReading) }, `{"sensor":"pressure-42","value":1e1000}`, true},
		{"trailing data is still refused", func() any { return new(numberReading) }, `{"sensor":"pressure-42","value":12} {}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			codec, err := edax.NewJSONCodec(map[string]func() any{"Reading": test.constructor})
			if err != nil {
				t.Fatal(err)
			}
			want := test.constructor()
			plainErr := json.Unmarshal([]byte(test.wire), want)
			got, err := codec.Unmarshal(edax.Payload{Name: "Reading", Version: 1, Data: []byte(test.wire)})
			if test.refuse {
				if plainErr == nil || !errors.Is(err, edax.ErrCodec) {
					t.Fatalf("decoded %+v: %v (stdlib: %v)", got, err, plainErr)
				}
			} else if plainErr != nil || err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("decoded %+v: %v, want %+v: %v", got, err, want, plainErr)
			}
		})
	}
}

func TestQueryPagesFloatsAsPython(t *testing.T) {
	big, wide := 9007199254740992.0, 5.808820077685227e16
	// The cursors are the Python SDK's for the same rows.
	for _, test := range []struct {
		name      string
		documents []edax.Document
		spec      edax.QuerySpec
		ids       []string
		next      string
	}{
		{"whole float below 2^53", []edax.Document{{"id": "a", "n": 2.0}, {"id": "b", "n": 2.0}},
			edax.QuerySpec{Sort: []edax.SortKey{{Path: "n"}}, Limit: edax.Ptr(uint64(1))}, []string{"a"}, "eyJzIjoiNjM0Yzc2N2U0Y2NlNTE2MiIsInYiOlsyLCJhIl19"},
		{"whole float at 2^53", []edax.Document{{"id": "a", "n": big}, {"id": "b", "n": big}},
			edax.QuerySpec{Sort: []edax.SortKey{{Path: "n"}}, Limit: edax.Ptr(uint64(1))}, []string{"a"}, "eyJzIjoiNjM0Yzc2N2U0Y2NlNTE2MiIsInYiOls5MDA3MTk5MjU0NzQwOTkyLCJhIl19"},
		{"whole float past 2^53", []edax.Document{{"id": "a", "n": wide}, {"id": "b", "n": wide}},
			edax.QuerySpec{Sort: []edax.SortKey{{Path: "n"}}, Limit: edax.Ptr(uint64(1))}, []string{"a"}, "eyJzIjoiNjM0Yzc2N2U0Y2NlNTE2MiIsInYiOls1ODA4ODIwMDc3Njg1MjI3MCwiYSJdfQ"},
		{"whole float at 1e20", []edax.Document{{"id": "a", "n": 1e20}, {"id": "b", "n": 1e20}},
			edax.QuerySpec{Sort: []edax.SortKey{{Path: "n"}}, Limit: edax.Ptr(uint64(1))}, []string{"a"}, "eyJzIjoiNjM0Yzc2N2U0Y2NlNTE2MiIsInYiOlsxMDAwMDAwMDAwMDAwMDAwMDAwMDAsImEiXX0"},
		// The float is 58088200776852272 exactly, so it sorts after the integer below it.
		{"float sorts with integers by value", []edax.Document{{"id": "float", "n": wide}, {"id": "integer", "n": uint64(wide) - 1}},
			edax.QuerySpec{Sort: []edax.SortKey{{Path: "n"}}}, []string{"integer", "float"}, ""},
		{"filter compares a whole float past 2^53", []edax.Document{{"id": "a", "n": big}, {"id": "b", "n": 1.5}},
			edax.QuerySpec{Filter: edax.Equal("n", big)}, []string{"a"}, ""},
		{"NaN is a null field", []edax.Document{{"id": "a", "n": math.NaN()}, {"id": "b", "n": 1.5}},
			edax.QuerySpec{Filter: edax.Exists("n", false)}, []string{"a"}, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			page, err := test.spec.Apply(test.documents)
			if err != nil {
				t.Fatal(err)
			}
			ids := []string{}
			for _, row := range page.Rows {
				ids = append(ids, row["id"].(string))
			}
			next := ""
			if page.Next != nil {
				next = *page.Next
			}
			if !reflect.DeepEqual(ids, test.ids) || next != test.next {
				t.Fatalf("ids %v next %q", ids, next)
			}
		})
	}
}
