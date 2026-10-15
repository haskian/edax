package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func parseFixture(t *testing.T, input string) (*binding, error) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", input, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	return parseBinding([]*ast.File{file})
}

func TestSignatureFormattingDoesNotChangeFacade(t *testing.T) {
	var prior []byte
	for _, name := range []string{"plain", "grouped"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/" + name + ".go")
			if err != nil {
				t.Fatal(err)
			}
			api, err := parseFixture(t, string(raw))
			if err != nil {
				t.Fatal(err)
			}
			if !api.Handles["Ledger"] || !api.Records["Payload"] || len(api.Functions) != 5 {
				t.Fatalf("wrong declarations: %+v", api)
			}
			fn := api.Functions[1]
			if fn.Receiver != "Ledger" || len(fn.Params) != 4 || fn.Params[0].Name != "first" || fn.Params[1].Name != "second" || len(fn.Results) != 2 {
				t.Fatalf("wrong flattened signature: %+v", fn)
			}
			ir := intermediateRepresentation{Functions: []irFunction{{Name: "versions", Params: []irParameter{{Name: "available", Optional: true}}}}}
			generated, err := renderFacade(api, ir)
			if err != nil {
				t.Fatal(err)
			}
			if prior != nil && !bytes.Equal(prior, generated) {
				t.Fatal("comments, grouping or named results changed generated facade")
			}
			prior = generated
			for _, fragment := range []string{
				"func (h *Ledger) Apply(first string, second string, note *string, rows []Payload) (*Payload, error)",
				`checkNativeString("first", first)`, `checkNativeString("second", second)`, `checkNativeString("note", *note)`,
				"h.inner.Apply(first, second, note, normalizeNative(rows))",
				"native.VersionsPresent(available != nil, normalizeNative(available))", "native.GetRecord(data, normalizeNative(labels))",
				"return wrapLedger(nativeValue)", "defer runtime.KeepAlive(h)",
			} {
				if !strings.Contains(string(generated), fragment) {
					t.Fatalf("missing %q", fragment)
				}
			}
		})
	}
}

func TestUnsupportedSignaturesFailClearly(t *testing.T) {
	for _, test := range []struct{ name, input, want string }{
		{"multiple values", "func Values() (int, string, error) {return 0,\"\",nil}", "at most one value"},
		{"grouped results", "func Values() (first, second string) {return \"\",\"\"}", "at most one value"},
		{"variadic", "func Values(values ...string) {}", "variadic"},
		{"unnamed input", "func Values(string) {}", "must be named"},
		{"generic", "func Values[T any](value T) T {return value}", "generic"},
		{"reserved local", "func Values(zero string) {}", "conflicts"},
		{"value handle", "type Ledger struct {ptr C.Ledger}; func (l Ledger) Values() {}", "value receiver"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseFixture(t, "package native\n"+test.input)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want %q", err, test.want)
			}
		})
	}
}
