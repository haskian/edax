package main

import (
	"encoding/json"
	"strings"
)

type irParameter struct {
	Name     string `json:"name"`
	Optional bool   `json:"optional"`
}
type irFunction struct {
	Name       string        `json:"name"`
	Params     []irParameter `json:"params"`
	ReturnType irValueType   `json:"return_type"`
}
type irType struct {
	Name    string       `json:"name"`
	Methods []irFunction `json:"methods"`
}
type intermediateRepresentation struct {
	Functions []irFunction `json:"functions"`
	Types     []irType     `json:"types"`
}

// Alef encodes scalar types as strings and compound types as tagged objects.
// Only optional named results need special ownership handling here.
type irValueType struct {
	Named    string       `json:"Named"`
	Optional *irValueType `json:"Optional"`
}

func (t *irValueType) UnmarshalJSON(data []byte) error {
	var scalar string
	if err := json.Unmarshal(data, &scalar); err == nil {
		*t = irValueType{}
		return nil
	}
	type object irValueType
	return json.Unmarshal(data, (*object)(t))
}

type methodKey struct{ Receiver, Method string }
type bindingMetadata map[methodKey]irFunction

func normalized(name string) string { return strings.ToLower(strings.ReplaceAll(name, "_", "")) }

func describeBinding(ir intermediateRepresentation) bindingMetadata {
	result := make(bindingMetadata)
	add := func(receiver string, functions []irFunction) {
		for _, fn := range functions {
			result[methodKey{normalized(receiver), normalized(fn.Name)}] = fn
		}
	}
	add("", ir.Functions)
	for _, typ := range ir.Types {
		add(typ.Name, typ.Methods)
	}
	return result
}

func (m bindingMetadata) function(fn function) irFunction {
	key := methodKey{normalized(fn.Receiver), normalized(fn.Name)}
	if found, ok := m[key]; ok {
		return found
	}
	// Alef prefixes free functions that collide with a type name with Get.
	if fn.Receiver == "" {
		key.Method = normalized(strings.TrimPrefix(fn.Name, "Get"))
	}
	return m[key]
}

func (m bindingMetadata) optional(fn function, parameter string) bool {
	for _, p := range m.function(fn).Params {
		if normalized(p.Name) == normalized(parameter) {
			return p.Optional
		}
	}
	return false
}
