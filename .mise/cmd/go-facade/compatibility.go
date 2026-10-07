package main

import (
	"fmt"
	"go/ast"
	"go/token"
)

type callPlan struct {
	Name      string
	Arguments []ast.Expr
	Unwrap    bool
}

// Alef's optional slices lose presence, and optional method records lose ownership.
// The ABI supplies companions with uniform suffixes; validate their complete shapes
// before choosing them. New operations must never silently take the unsafe path.
func planCall(api *binding, fn function, metadata bindingMetadata, arguments []ast.Expr) (callPlan, error) {
	plan := callPlan{Name: fn.Name, Arguments: arguments}
	presence := -1
	for i, p := range fn.Params {
		slice, ok := p.Type.(*ast.ArrayType)
		if ok && slice.Len == nil && metadata.optional(fn, p.Name) {
			if presence >= 0 {
				return plan, fmt.Errorf("multiple optional slices need an explicit ABI presence convention")
			}
			presence = i
		}
	}
	result := metadata.function(fn).ReturnType
	optionalRecord := fn.Receiver != "" && result.Optional != nil && result.Optional.Named != ""
	if presence < 0 && !optionalRecord {
		return plan, nil
	}
	if presence >= 0 && optionalRecord {
		return plan, fmt.Errorf("combined optional slice and record result needs an explicit ABI companion convention")
	}
	suffix := "Present"
	if optionalRecord {
		suffix = "Result"
	}
	companion, err := findCompanion(api, fn, metadata, suffix)
	if err != nil {
		return plan, err
	}
	plan.Name = companion.Name
	fail := func() (callPlan, error) {
		return plan, fmt.Errorf("incompatible companion %s.%s", fn.Receiver, plan.Name)
	}
	if optionalRecord {
		if len(fn.Results) == 0 || !sameType(fn.Results[0], &ast.StarExpr{X: ast.NewIdent(result.Optional.Named)}) ||
			!sameParameters(fn.Params, companion.Params) || !owningEnvelope(api, fn, companion) ||
			metadata.function(companion).ReturnType.Optional != nil {
			return fail()
		}
		plan.Unwrap = true
		return plan, nil
	}
	if len(companion.Params) != len(fn.Params)+1 || !sameTypes(fn.Results, companion.Results) {
		return fail()
	}
	bit := companion.Params[presence]
	parameter := fn.Params[presence]
	if !isIdent(bit.Type, "bool") || (bit.Name != "present" && bit.Name != parameter.Name+"Present") {
		return fail()
	}
	if !sameParameters(fn.Params[:presence], companion.Params[:presence]) ||
		!sameParameters(fn.Params[presence:], companion.Params[presence+1:]) {
		return fail()
	}
	for _, p := range companion.Params {
		wantOptional := p.Name != parameter.Name && p.Name != bit.Name && metadata.optional(fn, p.Name)
		if metadata.optional(companion, p.Name) != wantOptional {
			return fail()
		}
	}
	plan.Arguments = append([]ast.Expr(nil), arguments[:presence]...)
	plan.Arguments = append(plan.Arguments,
		&ast.BinaryExpr{X: ast.NewIdent(parameter.Name), Op: token.NEQ, Y: ast.NewIdent("nil")},
		call("normalizeNative", ast.NewIdent(parameter.Name)),
	)
	plan.Arguments = append(plan.Arguments, arguments[presence+1:]...)
	return plan, nil
}

func findCompanion(api *binding, original function, metadata bindingMetadata, suffix string) (function, error) {
	want := normalized(metadata.function(original).Name + suffix)
	for _, fn := range api.Functions {
		name := metadata.function(fn).Name
		if name == "" {
			name = fn.Name
		}
		if fn.Receiver == original.Receiver && normalized(name) == want {
			return fn, nil
		}
	}
	return function{}, fmt.Errorf("missing %s companion for %s.%s", suffix, original.Receiver, original.Name)
}

func owningEnvelope(api *binding, original, companion function) bool {
	if len(companion.Results) == 0 || len(original.Results) != len(companion.Results) ||
		!sameTypes(original.Results[1:], companion.Results[1:]) {
		return false
	}
	pointer, ok := companion.Results[0].(*ast.StarExpr)
	if !ok {
		return false
	}
	name, ok := pointer.X.(*ast.Ident)
	if !ok || !api.Records[name.Name] {
		return false
	}
	envelope := api.Structures[name.Name]
	if envelope == nil || len(envelope.Fields.List) != 1 {
		return false
	}
	field := envelope.Fields.List[0]
	return len(field.Names) == 1 && field.Names[0].Name == "Value" && sameType(field.Type, original.Results[0])
}

func sameParameters(left, right []parameter) bool {
	if len(left) != len(right) {
		return false
	}
	for i, p := range left {
		if p.Name != right[i].Name || !sameType(p.Type, right[i].Type) {
			return false
		}
	}
	return true
}

func sameTypes(left, right []ast.Expr) bool {
	if len(left) != len(right) {
		return false
	}
	for i, typ := range left {
		if !sameType(typ, right[i]) {
			return false
		}
	}
	return true
}

func sameType(left, right ast.Expr) bool {
	l, err := source(left)
	if err != nil {
		return false
	}
	r, err := source(right)
	return err == nil && l == r
}
