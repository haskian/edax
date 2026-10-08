package edax

import (
	"encoding"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

var messagePackDecoders = []reflect.Type{
	reflect.TypeFor[msgpack.CustomDecoder](),
	reflect.TypeFor[msgpack.Unmarshaler](),
	reflect.TypeFor[encoding.BinaryUnmarshaler](),
	reflect.TypeFor[encoding.TextUnmarshaler](),
}

func hasMessagePackDecoder(typ reflect.Type) bool {
	for _, decoder := range messagePackDecoders {
		if typ.Implements(decoder) || reflect.PointerTo(typ).Implements(decoder) {
			return true
		}
	}
	return false
}

// The library reads an array as a positional struct, nil as any zero value, and truncates integers.
// Check only automatic conversion here; custom decoders keep their own wire contract.
func checkMessagePackShape(decoder *msgpack.Decoder, typ reflect.Type) error {
	if hasMessagePackDecoder(typ) {
		return decoder.Skip()
	}
	code, err := decoder.PeekCode()
	if err != nil {
		return err
	}
	if code == msgpcode.Nil {
		if nullable(typ) {
			return decoder.Skip()
		}
		return fmt.Errorf("%s cannot decode from nil", typ)
	}
	switch typ.Kind() {
	case reflect.Pointer:
		return checkMessagePackShape(decoder, typ.Elem())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return checkMessagePackInteger(decoder, code, typ)
	case reflect.Struct:
		if !msgpcode.IsFixedMap(code) && code != msgpcode.Map16 && code != msgpcode.Map32 {
			return fmt.Errorf("%s must decode from a MessagePack map", typ)
		}
		size, err := decoder.DecodeMapLen()
		if err != nil {
			return err
		}
		fields := messagePackFields(typ, map[reflect.Type]bool{})
		for range size {
			name, err := decoder.DecodeString()
			if err != nil {
				return err
			}
			if field, ok := fields[name]; ok {
				err = checkMessagePackShape(decoder, field)
			} else {
				err = decoder.Skip()
			}
			if err != nil {
				return err
			}
		}
	case reflect.Array, reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			return decoder.Skip()
		}
		size, err := decoder.DecodeArrayLen()
		if err != nil {
			return err
		}
		for range size {
			if err := checkMessagePackShape(decoder, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.Map:
		size, err := decoder.DecodeMapLen()
		if err != nil {
			return err
		}
		for range size {
			if err := checkMessagePackShape(decoder, typ.Key()); err != nil {
				return err
			}
			if err := checkMessagePackShape(decoder, typ.Elem()); err != nil {
				return err
			}
		}
	default:
		return decoder.Skip()
	}
	return nil
}

// The library truncates an integer to fit its field: 256 reads as uint8 0, and -1 as uint8 255.
// encoding/json refuses both, so this check does too.
func checkMessagePackInteger(decoder *msgpack.Decoder, code byte, typ reflect.Type) error {
	if !msgpcode.IsFixedNum(code) && (code < msgpcode.Uint8 || code > msgpcode.Int64) {
		return decoder.Skip()
	}
	value, err := decoder.DecodeInterface()
	if err != nil {
		return err
	}
	number, field := reflect.ValueOf(value), reflect.New(typ).Elem()
	var fits bool
	switch {
	case number.CanInt() && field.CanInt():
		fits = !field.OverflowInt(number.Int())
	case number.CanInt():
		fits = number.Int() >= 0 && !field.OverflowUint(uint64(number.Int()))
	case field.CanInt():
		fits = number.Uint() <= math.MaxInt64 && !field.OverflowInt(int64(number.Uint()))
	default:
		fits = !field.OverflowUint(number.Uint())
	}
	if !fits {
		return fmt.Errorf("%v overflows %s", value, typ)
	}
	return nil
}

// positionalStruct finds a struct whose `_msgpack` field sets as_array. The library writes such a
// struct as an array, and every SDK reads a struct from a map only, so no reader would accept it.
func positionalStruct(typ reflect.Type, seen map[reflect.Type]bool) reflect.Type {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if seen[typ] || hasMessagePackDecoder(typ) {
		return nil
	}
	seen[typ] = true
	switch typ.Kind() {
	case reflect.Struct:
		if field, ok := typ.FieldByName("_msgpack"); ok && len(field.Index) == 1 {
			tag := field.Tag.Get("msgpack")
			if tag == "" {
				tag = field.Tag.Get("json")
			}
			for _, option := range strings.Split(tag, ",")[1:] {
				if option == "as_array" || option == "asArray" {
					return typ
				}
			}
		}
		for _, field := range messagePackFields(typ, map[reflect.Type]bool{}) {
			if found := positionalStruct(field, seen); found != nil {
				return found
			}
		}
	case reflect.Array, reflect.Slice, reflect.Map:
		return positionalStruct(typ.Elem(), seen)
	}
	return nil
}

// Resolve names using msgpack tags first and json as the configured fallback.
// Embedded automatic structs inline unless noinline or an existing name prevents it.
func messagePackFields(typ reflect.Type, visiting map[reflect.Type]bool) map[string]reflect.Type {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	fields := make(map[string]reflect.Type)
	if typ.Kind() != reflect.Struct || visiting[typ] {
		return fields
	}
	visiting[typ] = true
	defer delete(visiting, typ)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("msgpack")
		if tag == "" {
			tag = field.Tag.Get("json")
		}
		parts := strings.Split(tag, ",")
		name := parts[0]
		if name == "-" || field.PkgPath != "" && !field.Anonymous {
			continue
		}
		options := make(map[string]string)
		for _, part := range parts[1:] {
			key, value, _ := strings.Cut(part, ":")
			options[key] = value
		}
		if name == "" {
			name = field.Name
		}
		_, noinline := options["noinline"]
		if field.Anonymous && !noinline && !hasMessagePackDecoder(field.Type) {
			nested := messagePackFields(field.Type, visiting)
			_, forced := options["inline"]
			conflict := false
			for key := range nested {
				if _, exists := fields[key]; exists {
					conflict = true
					break
				}
			}
			if forced || !conflict {
				for key, nestedType := range nested {
					if _, exists := fields[key]; !exists {
						fields[key] = nestedType
					}
				}
			}
		}
		fields[name] = field.Type
		if alias, ok := options["alias"]; ok {
			fields[alias] = field.Type
		}
	}
	return fields
}
