package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

type bindingType struct {
	Name   string
	Handle bool
}
type parameter struct {
	Name string
	Type ast.Expr
}
type function struct {
	Receiver, Name string
	Params         []parameter
	Results        []ast.Expr
}
type binding struct {
	Types      []bindingType
	Functions  []function
	Handles    map[string]bool
	Records    map[string]bool
	Structures map[string]*ast.StructType
}

func readBinding(directory string) (*binding, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var files []*ast.File
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		if file.Name.Name == "native" {
			files = append(files, file)
		}
	}
	return parseBinding(files)
}

func parseBinding(files []*ast.File) (*binding, error) {
	api := &binding{Handles: make(map[string]bool), Records: make(map[string]bool), Structures: make(map[string]*ast.StructType)}
	for _, file := range files {
		for _, declaration := range file.Decls {
			group, ok := declaration.(*ast.GenDecl)
			if !ok || group.Tok != token.TYPE {
				continue
			}
			for _, specification := range group.Specs {
				spec := specification.(*ast.TypeSpec)
				if !spec.Name.IsExported() {
					continue
				}
				structure, ok := spec.Type.(*ast.StructType)
				if !ok || spec.TypeParams != nil {
					return nil, fmt.Errorf("unsupported binding type %s: expected a non-generic struct", spec.Name.Name)
				}
				handle := false
				for _, field := range structure.Fields.List {
					for _, name := range field.Names {
						if name.Name != "ptr" {
							continue
						}
						typ := field.Type
						if pointer, ok := typ.(*ast.StarExpr); ok {
							typ = pointer.X
						}
						selector, ok := typ.(*ast.SelectorExpr)
						if ok && isIdent(selector.X, "C") {
							handle = true
						}
					}
				}
				name := spec.Name.Name
				if api.Handles[name] || api.Records[name] {
					return nil, fmt.Errorf("duplicate binding type %s", name)
				}
				api.Types = append(api.Types, bindingType{Name: name, Handle: handle})
				api.Structures[name] = structure
				if handle {
					api.Handles[name] = true
				} else {
					api.Records[name] = true
				}
			}
		}
	}
	for _, file := range files {
		for _, declaration := range file.Decls {
			decl, ok := declaration.(*ast.FuncDecl)
			// Alef helpers are not ABI operations; the facade owns its conversions.
			if !ok || !decl.Name.IsExported() || (decl.Name.Name == "UnmarshalBytes" || decl.Name.Name == "Ptr") {
				continue
			}
			receiver := ""
			if decl.Recv != nil {
				if len(decl.Recv.List) != 1 {
					return nil, fmt.Errorf("unsupported receiver on %s", decl.Name.Name)
				}
				receiverType := decl.Recv.List[0].Type
				pointer, pointerReceiver := receiverType.(*ast.StarExpr)
				if pointerReceiver {
					receiverType = pointer.X
				}
				name, ok := receiverType.(*ast.Ident)
				if !ok {
					return nil, fmt.Errorf("unsupported receiver on %s", decl.Name.Name)
				}
				if !api.Handles[name.Name] || decl.Name.Name == "Free" {
					continue
				}
				if !pointerReceiver {
					return nil, fmt.Errorf("unsupported value receiver on handle %s.%s", name.Name, decl.Name.Name)
				}
				receiver = name.Name
			}
			fn, err := parseFunction(receiver, decl)
			if err != nil {
				return nil, err
			}
			api.Functions = append(api.Functions, fn)
		}
	}
	return api, nil
}

func parseFunction(receiver string, decl *ast.FuncDecl) (function, error) {
	fn := function{Receiver: receiver, Name: decl.Name.Name}
	fail := func(reason string) (function, error) {
		return function{}, fmt.Errorf("unsupported signature %s.%s: %s", receiver, fn.Name, reason)
	}
	if decl.Type.TypeParams != nil {
		return fail("generic functions are not supported")
	}
	if decl.Type.Params != nil {
		for _, field := range decl.Type.Params.List {
			if _, ok := field.Type.(*ast.Ellipsis); ok {
				return fail("variadic parameters are not supported")
			}
			if len(field.Names) == 0 {
				return fail("parameters must be named")
			}
			for _, name := range field.Names {
				if name.Name == "_" || name.Name == "h" || name.Name == "zero" || name.Name == "nativeValue" || name.Name == "nativeError" {
					return fail("parameter name conflicts with generated locals: " + name.Name)
				}
				fn.Params = append(fn.Params, parameter{Name: name.Name, Type: field.Type})
			}
		}
	}
	if decl.Type.Results != nil {
		for _, field := range decl.Type.Results.List {
			count := max(1, len(field.Names))
			for range count {
				fn.Results = append(fn.Results, field.Type)
			}
		}
	}
	values := len(fn.Results)
	if values > 0 && isIdent(fn.Results[values-1], "error") {
		values--
	}
	if values > 1 {
		return fail("at most one value followed by an optional error is supported")
	}
	for _, typ := range fn.Results[:values] {
		if isIdent(typ, "error") {
			return fail("error must be the final result")
		}
	}
	return fn, nil
}

func isIdent(expression ast.Expr, name string) bool {
	id, ok := expression.(*ast.Ident)
	return ok && id.Name == name
}

func source(node ast.Node) (string, error) {
	var output bytes.Buffer
	if err := format.Node(&output, token.NewFileSet(), node); err != nil {
		return "", err
	}
	return output.String(), nil
}
