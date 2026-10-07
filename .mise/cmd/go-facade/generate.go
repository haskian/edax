package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"os"
	"path/filepath"
	"slices"
	"text/template"
)

type textCheck struct {
	Name    string
	Pointer bool
}
type wrapper struct {
	Name, Receiver, Signature, Value, Zero, Call, Result string
	Fallible, Checked, ReturnsError, Unwrap              bool
	StringChecks                                         []textCheck
	ValueChecks                                          []string
}
type facade struct {
	Types     []bindingType
	Functions []wrapper
	Handles   []string
}

func generateFacade(root string) error {
	directory := filepath.Join(root, "crates/edax-go")
	api, err := readBinding(filepath.Join(directory, "internal/native"))
	if err != nil {
		return fmt.Errorf("read native binding: %w", err)
	}
	if len(api.Handles) == 0 || len(api.Functions) < 50 {
		return fmt.Errorf("native Go API is missing or unexpectedly incomplete: %d handles, %d functions", len(api.Handles), len(api.Functions))
	}
	raw, err := os.ReadFile(filepath.Join(root, ".alef/edax-abi/ir.json"))
	if err != nil {
		return err
	}
	var ir intermediateRepresentation
	if err := json.Unmarshal(raw, &ir); err != nil {
		return fmt.Errorf("read alef IR: %w", err)
	}
	output, err := renderFacade(api, ir)
	if err != nil {
		return err
	}
	destination := filepath.Join(directory, "internal/kernel")
	if err := os.MkdirAll(destination, 0755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(destination, "native.gen.go"), output, 0644)
}

func renderFacade(api *binding, ir intermediateRepresentation) ([]byte, error) {
	metadata := describeBinding(ir)
	model := facade{Types: api.Types}
	for _, fn := range api.Functions {
		wrapper, err := makeWrapper(api, fn, metadata)
		if err != nil {
			return nil, fmt.Errorf("generate %s.%s: %w", fn.Receiver, fn.Name, err)
		}
		model.Functions = append(model.Functions, wrapper)
	}
	for name := range api.Handles {
		model.Handles = append(model.Handles, name)
	}
	slices.Sort(model.Handles)
	tmpl, err := template.New("facade").Parse(facadeTemplate)
	if err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := tmpl.Execute(&output, model); err != nil {
		return nil, err
	}
	formatted, err := format.Source(output.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format generated facade: %w", err)
	}
	return formatted, nil
}

func makeWrapper(api *binding, fn function, metadata bindingMetadata) (wrapper, error) {
	w := wrapper{Name: fn.Name, Receiver: fn.Receiver}
	if api.Handles[fn.Name] {
		w.Name = "New" + fn.Name
	}
	results := append([]ast.Expr(nil), fn.Results...)
	if len(results) > 0 && isIdent(results[len(results)-1], "error") {
		w.Fallible = true
		results = results[:len(results)-1]
	}
	var arguments []ast.Expr
	var parameters []*ast.Field
	for _, parameter := range fn.Params {
		typ := parameter.Type
		base := typ
		pointer, indirect := typ.(*ast.StarExpr)
		if indirect {
			base = pointer.X
		}
		if isIdent(base, "string") {
			w.StringChecks = append(w.StringChecks, textCheck{Name: parameter.Name, Pointer: indirect})
		} else if needsNormalization(typ, api.Records) && !byteSlice(typ) {
			w.ValueChecks = append(w.ValueChecks, parameter.Name)
		}
		argument := ast.Expr(ast.NewIdent(parameter.Name))
		if needsNormalization(typ, api.Records) && !metadata.optional(fn, parameter.Name) {
			argument = call("normalizeNative", argument)
		}
		arguments = append(arguments, argument)
		parameters = append(parameters, &ast.Field{Names: []*ast.Ident{ast.NewIdent(parameter.Name)}, Type: typ})
	}
	w.Checked = len(w.StringChecks) > 0 || len(w.ValueChecks) > 0
	w.ReturnsError = w.Fallible || fn.Receiver != "" || w.Checked
	if len(results) > 0 {
		value, err := source(results[0])
		if err != nil {
			return wrapper{}, err
		}
		w.Value = value
		if fn.Receiver != "" || w.Checked {
			w.Zero = "zero, "
		}
	}
	if w.ReturnsError {
		results = append(results, ast.NewIdent("error"))
	}
	declaration := &ast.FuncDecl{Name: ast.NewIdent(w.Name), Type: &ast.FuncType{Params: &ast.FieldList{List: parameters}}}
	for _, result := range results {
		if declaration.Type.Results == nil {
			declaration.Type.Results = &ast.FieldList{}
		}
		declaration.Type.Results.List = append(declaration.Type.Results.List, &ast.Field{Type: result})
	}
	plan, err := planCall(api, fn, metadata, arguments)
	if err != nil {
		return wrapper{}, err
	}
	w.Unwrap = plan.Unwrap
	target := &ast.SelectorExpr{X: ast.NewIdent("native"), Sel: ast.NewIdent(plan.Name)}
	if fn.Receiver != "" {
		declaration.Recv = &ast.FieldList{List: []*ast.Field{{Names: []*ast.Ident{ast.NewIdent("h")}, Type: &ast.StarExpr{X: ast.NewIdent(fn.Receiver)}}}}
		target.X = &ast.SelectorExpr{X: ast.NewIdent("h"), Sel: ast.NewIdent("inner")}
	}
	signature, err := source(declaration)
	if err != nil {
		return wrapper{}, err
	}
	w.Signature = signature
	invocation, err := source(&ast.CallExpr{Fun: target, Args: plan.Arguments})
	if err != nil {
		return wrapper{}, err
	}
	w.Call = invocation
	result := ast.Expr(ast.NewIdent("nativeValue"))
	if w.Unwrap {
		result = &ast.SelectorExpr{X: result, Sel: ast.NewIdent("Value")}
	}
	if len(fn.Results) > 0 {
		if pointer, ok := fn.Results[0].(*ast.StarExpr); ok {
			if name, ok := pointer.X.(*ast.Ident); ok && api.Handles[name.Name] {
				result = call("wrap"+name.Name, ast.NewIdent("nativeValue"))
			}
		}
	}
	w.Result, err = source(result)
	return w, err
}

func needsNormalization(typ ast.Expr, records map[string]bool) bool {
	if pointer, ok := typ.(*ast.StarExpr); ok {
		typ = pointer.X
	}
	switch typ := typ.(type) {
	case *ast.ArrayType:
		return typ.Len == nil
	case *ast.MapType:
		return true
	case *ast.Ident:
		return records[typ.Name]
	default:
		return false
	}
}
func byteSlice(typ ast.Expr) bool {
	slice, ok := typ.(*ast.ArrayType)
	return ok && slice.Len == nil && isIdent(slice.Elt, "byte")
}
func call(name string, args ...ast.Expr) ast.Expr {
	return &ast.CallExpr{Fun: ast.NewIdent(name), Args: args}
}
