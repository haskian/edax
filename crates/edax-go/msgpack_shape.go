package edax

import (
	"encoding"
	"fmt"
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

// The library accepts arrays as positional structs without an opt-out. Check only
// automatic struct conversion here; custom decoders retain their own wire contract.
func checkMessagePackShape(decoder *msgpack.Decoder, typ reflect.Type) error {
	if hasMessagePackDecoder(typ) {
		return decoder.Skip()
	}
	code, err := decoder.PeekCode()
	if err != nil {
		return err
	}
	if typ.Kind() == reflect.Pointer {
		if code == msgpcode.Nil {
			return decoder.Skip()
		}
		return checkMessagePackShape(decoder, typ.Elem())
	}
	switch typ.Kind() {
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
		if typ.Elem().Kind() == reflect.Uint8 || code == msgpcode.Nil {
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
		if code == msgpcode.Nil {
			return decoder.Skip()
		}
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
