package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCompatibilityUsesDeclarations(t *testing.T) {
	const input = `package native
type Ledger struct { ptr C.Ledger }
type Payload struct { Name string }
type LookupReply struct { Value *Payload }
func (l *Ledger) Lookup(key string) (*Payload, error) { return nil, nil }
func (l *Ledger) LookupResult(key string) (*LookupReply, error) { return nil, nil }
func (l *Ledger) Fetch() *Payload { return nil }
func (l *Ledger) FetchResult() *LookupReply { return nil }
func (l *Ledger) Count() *uint64 { return nil }
func (l *Ledger) Select(data []byte, rows []Payload) error { return nil }
func (l *Ledger) SelectPresent(data []byte, rowsPresent bool, rows []Payload) error { return nil }
func Resolve(data []byte, note *string, available []uint32) ([]uint32, error) { return nil, nil }
func ResolvePresent(data []byte, note *string, present bool, available []uint32) ([]uint32, error) { return nil, nil }
func Locate() *Payload { return nil }
`
	for _, receiver := range []string{"Ledger", "Catalog"} {
		t.Run(receiver, func(t *testing.T) {
			api, err := parseFixture(t, strings.ReplaceAll(input, "Ledger", receiver))
			if err != nil {
				t.Fatal(err)
			}
			ir := intermediateRepresentation{
				Types: []irType{{Name: receiver, Methods: []irFunction{
					{Name: "lookup", ReturnType: irValueType{Optional: &irValueType{Named: "Payload"}}},
					{Name: "fetch", ReturnType: irValueType{Optional: &irValueType{Named: "Payload"}}},
					{Name: "count", ReturnType: irValueType{Optional: &irValueType{}}},
					{Name: "select", Params: []irParameter{{Name: "rows", Optional: true}}},
				}}},
				Functions: []irFunction{
					{Name: "resolve", Params: []irParameter{{Name: "note", Optional: true}, {Name: "available", Optional: true}}},
					{Name: "resolve_present", Params: []irParameter{{Name: "note", Optional: true}}},
					{Name: "locate", ReturnType: irValueType{Optional: &irValueType{Named: "Payload"}}},
				},
			}
			generated, err := renderFacade(api, ir)
			if err != nil {
				t.Fatal(err)
			}
			for _, fragment := range []string{
				"func (h *" + receiver + ") Lookup(key string) (*Payload, error)",
				"h.inner.LookupResult(key)",
				"return nativeValue.Value, mapNativeError(nativeError)",
				"h.inner.FetchResult()",
				"return nativeValue.Value, nil",
				"h.inner.Count()",
				"h.inner.SelectPresent(normalizeNative(data), rows != nil, normalizeNative(rows))",
				"native.ResolvePresent(normalizeNative(data), note, available != nil, normalizeNative(available))",
				"native.Locate()",
			} {
				if !strings.Contains(string(generated), fragment) {
					t.Fatalf("missing generated behavior %q", fragment)
				}
			}
		})
	}
}

func TestReturnMetadataDecoding(t *testing.T) {
	for _, test := range []struct {
		name, input, record string
		optional            bool
	}{
		{"string primitive", `"String"`, "", false},
		{"tagged primitive", `{"Primitive":"U64"}`, "", false},
		{"record", `{"Named":"Payload"}`, "Payload", false},
		{"optional string", `{"Optional":"String"}`, "", true},
		{"optional tagged primitive", `{"Optional":{"Primitive":"U64"}}`, "", true},
		{"optional record", `{"Optional":{"Named":"Payload"}}`, "Payload", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var result irValueType
			if err := json.Unmarshal([]byte(test.input), &result); err != nil {
				t.Fatal(err)
			}
			if (result.Optional != nil) != test.optional {
				t.Fatalf("optional=%v, want %v", result.Optional != nil, test.optional)
			}
			if test.optional {
				result = *result.Optional
			}
			if result.Named != test.record {
				t.Fatalf("record=%q, want %q", result.Named, test.record)
			}
		})
	}
}

func TestPresenceCompanionWithTypeNameCollisions(t *testing.T) {
	for _, companion := range []string{"VersionsPresent", "GetVersionsPresent"} {
		t.Run(companion, func(t *testing.T) {
			input := `package native
type Versions struct{Number uint32}
func GetVersions(available []uint32) uint32 {return 0}
func ` + companion + `(present bool, available []uint32) uint32 {return 0}
`
			if strings.HasPrefix(companion, "Get") {
				input += "type VersionsPresent struct{Number uint32}\n"
			}
			api, err := parseFixture(t, input)
			if err != nil {
				t.Fatal(err)
			}
			ir := intermediateRepresentation{Functions: []irFunction{
				{Name: "versions", Params: []irParameter{{Name: "available", Optional: true}}},
				{Name: "versions_present"},
			}}
			generated, err := renderFacade(api, ir)
			if err != nil {
				t.Fatal(err)
			}
			want := "native." + companion + "(available != nil, normalizeNative(available))"
			if !strings.Contains(string(generated), want) {
				t.Fatalf("missing presence call %q", want)
			}
		})
	}
}

func TestOptionalEnvelopeCompanionFails(t *testing.T) {
	api, err := parseFixture(t, `package native
type Ledger struct{ptr C.Ledger}
type Payload struct{Name string}
type Reply struct{Value *Payload}
func (l *Ledger) Lookup() *Payload {return nil}
func (l *Ledger) LookupResult() *Reply {return nil}
`)
	if err != nil {
		t.Fatal(err)
	}
	ir := intermediateRepresentation{Types: []irType{{Name: "Ledger", Methods: []irFunction{
		{Name: "lookup", ReturnType: irValueType{Optional: &irValueType{Named: "Payload"}}},
		{Name: "lookup_result", ReturnType: irValueType{Optional: &irValueType{Named: "Reply"}}},
	}}}}
	_, err = renderFacade(api, ir)
	if err == nil || !strings.Contains(err.Error(), "Lookup") {
		t.Fatalf("expected optional Lookup envelope error, got %v", err)
	}
}

func TestMalformedPresenceCompanionFails(t *testing.T) {
	const original = "func Resolve(prefix string, values []byte) (bool, error) { return false, nil }\n"
	for _, test := range []struct{ name, companion string }{
		{"missing", ""},
		{"wrong presence type", "func ResolvePresent(prefix string, present string, values []byte) (bool, error) {return false,nil}"},
		{"wrong presence name", "func ResolvePresent(prefix string, enabled bool, values []byte) (bool, error) {return false,nil}"},
		{"wrong presence position", "func ResolvePresent(present bool, prefix string, values []byte) (bool, error) {return false,nil}"},
		{"wrong slice type", "func ResolvePresent(prefix string, present bool, values []string) (bool, error) {return false,nil}"},
		{"wrong slice name", "func ResolvePresent(prefix string, present bool, data []byte) (bool, error) {return false,nil}"},
		{"changed other argument", "func ResolvePresent(prefix []byte, present bool, values []byte) (bool, error) {return false,nil}"},
		{"missing other argument", "func ResolvePresent(present bool, values []byte) (bool, error) {return false,nil}"},
		{"extra argument", "func ResolvePresent(prefix string, present bool, values []byte, extra bool) (bool, error) {return false,nil}"},
		{"changed result", "func ResolvePresent(prefix string, present bool, values []byte) (string, error) {return \"\",nil}"},
		{"changed error arity", "func ResolvePresent(prefix string, present bool, values []byte) bool {return false}"},
		{"wrong receiver", "type Ledger struct{ptr C.Ledger}; func (l *Ledger) ResolvePresent(prefix string, present bool, values []byte) (bool,error) {return false,nil}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, err := parseFixture(t, "package native\n"+original+test.companion)
			if err != nil {
				t.Fatal(err)
			}
			ir := intermediateRepresentation{Functions: []irFunction{{Name: "resolve", Params: []irParameter{{Name: "values", Optional: true}}}}}
			_, err = renderFacade(api, ir)
			if err == nil || !strings.Contains(err.Error(), "Resolve") {
				t.Fatalf("expected Resolve companion error, got %v", err)
			}
		})
	}
}

func TestMalformedResultCompanionFails(t *testing.T) {
	const original = `package native
type Ledger struct {ptr C.Ledger}
type Payload struct {Name string}
func (l *Ledger) Lookup(key string) (*Payload,error) {return nil,nil}
`
	const validCompanion = "func (l *Ledger) LookupResult(key string) (*Reply,error) {return nil,nil}"
	for _, test := range []struct{ name, envelope, companion string }{
		{"missing", "type Reply struct{Value *Payload}", ""},
		{"missing value", "type Reply struct{Other *Payload}", validCompanion},
		{"wrong value type", "type Reply struct{Value *string}", validCompanion},
		{"nonoptional value", "type Reply struct{Value Payload}", validCompanion},
		{"extra field", "type Reply struct{Value *Payload; Other bool}", validCompanion},
		{"handle envelope", "type Reply struct{ptr C.Reply; Value *Payload}", validCompanion},
		{"wrong argument", "type Reply struct{Value *Payload}", "func (l *Ledger) LookupResult(id string) (*Reply,error) {return nil,nil}"},
		{"wrong error arity", "type Reply struct{Value *Payload}", "func (l *Ledger) LookupResult(key string) *Reply {return nil}"},
		{"wrong receiver", "type Reply struct{Value *Payload}; type Catalog struct{ptr C.Catalog}", "func (c *Catalog) LookupResult(key string) (*Reply,error) {return nil,nil}"},
		{"nonpointer envelope", "type Reply struct{Value *Payload}", "func (l *Ledger) LookupResult(key string) (Reply,error) {return Reply{},nil}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, err := parseFixture(t, original+test.envelope+"\n"+test.companion)
			if err != nil {
				t.Fatal(err)
			}
			ir := intermediateRepresentation{Types: []irType{{Name: "Ledger", Methods: []irFunction{{Name: "lookup", ReturnType: irValueType{Optional: &irValueType{Named: "Payload"}}}}}}}
			_, err = renderFacade(api, ir)
			if err == nil || !strings.Contains(err.Error(), "Lookup") {
				t.Fatalf("expected Lookup companion error, got %v", err)
			}
		})
	}
}

func TestUnsupportedCompatibilityCombinationsFail(t *testing.T) {
	for _, test := range []struct {
		name, declaration string
		metadata          irFunction
	}{
		{"multiple optional slices", "func (l *Ledger) Lookup(first []byte, second []byte) bool {return false}", irFunction{Name: "lookup", Params: []irParameter{{Name: "first", Optional: true}, {Name: "second", Optional: true}}}},
		{"optional slice and record", "func (l *Ledger) Lookup(values []byte) *Payload {return nil}", irFunction{Name: "lookup", Params: []irParameter{{Name: "values", Optional: true}}, ReturnType: irValueType{Optional: &irValueType{Named: "Payload"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			api, err := parseFixture(t, "package native\ntype Ledger struct{ptr C.Ledger}\ntype Payload struct{Name string}\n"+test.declaration)
			if err != nil {
				t.Fatal(err)
			}
			_, err = renderFacade(api, intermediateRepresentation{Types: []irType{{Name: "Ledger", Methods: []irFunction{test.metadata}}}})
			if err == nil || !strings.Contains(err.Error(), "Lookup") {
				t.Fatalf("expected Lookup compatibility error, got %v", err)
			}
		})
	}
}
