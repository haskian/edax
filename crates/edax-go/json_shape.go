package edax

import (
	"bytes"
	"cmp"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"unicode"
)

var jsonDecoders = []reflect.Type{
	reflect.TypeFor[json.Unmarshaler](),
	reflect.TypeFor[encoding.TextUnmarshaler](),
}

// nullable reports whether Go has a nil for typ. Every other type reads null as its zero value,
// which the shared contract refuses: a struct reads from a map only, and a scalar is never absent.
func nullable(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
		return true
	}
	return false
}

// decodeShapedJSON is the typed JSON codec's decoder. It first refuses the null that encoding/json
// reads into a non-nullable type, at the root or nested. A type with its own decoder keeps its own
// contract.
func decodeShapedJSON(data []byte, value any) error {
	var shape any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&shape); err != nil {
		return err
	}
	if err := checkJSONValue(shape, reflect.TypeOf(value).Elem()); err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func checkJSONValue(value any, typ reflect.Type) error {
	for _, decoder := range jsonDecoders {
		if typ.Implements(decoder) || reflect.PointerTo(typ).Implements(decoder) {
			return nil
		}
	}
	if value == nil {
		if nullable(typ) {
			return nil
		}
		return fmt.Errorf("%s cannot decode from null", typ)
	}
	// A value of the wrong JSON type passes here: encoding/json refuses it itself.
	switch typ.Kind() {
	case reflect.Pointer:
		return checkJSONValue(value, typ.Elem())
	case reflect.Struct:
		object, _ := value.(map[string]any)
		fields := jsonFields(typ)
		for name, member := range object {
			field, ok := jsonFieldNamed(fields, name)
			if !ok {
				continue
			}
			if err := checkJSONValue(member, typ.FieldByIndex(field.index).Type); err != nil {
				return err
			}
		}
	case reflect.Array, reflect.Slice:
		items, _ := value.([]any)
		for _, item := range items {
			if err := checkJSONValue(item, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.Map:
		object, _ := value.(map[string]any)
		for _, member := range object {
			if err := checkJSONValue(member, typ.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

// jsonFieldNamed matches a key as encoding/json does: the exact name first, then the first
// case-insensitive match.
func jsonFieldNamed(fields []jsonField, name string) (jsonField, bool) {
	for _, field := range fields {
		if field.name == name {
			return field, true
		}
	}
	for _, field := range fields {
		if strings.EqualFold(field.name, name) {
			return field, true
		}
	}
	return jsonField{}, false
}

type jsonField struct {
	name   string
	index  []int
	tagged bool
	// The encoder honours these options. The decoder ignores them, as encoding/json does.
	omitEmpty, omitZero, quoted bool
}

var jsonFieldCache sync.Map // reflect.Type → []jsonField

// jsonFields resolves a struct's JSON members by encoding/json's rules: tags, embedded structs,
// and Go's embedding dominance, where a shallower or tagged name hides the others and two equal
// claims to one name hide each other.
func jsonFields(typ reflect.Type) []jsonField {
	if cached, ok := jsonFieldCache.Load(typ); ok {
		return cached.([]jsonField)
	}
	type embedded struct {
		typ   reflect.Type
		index []int
	}
	var fields []jsonField
	next, visited := []embedded{{typ: typ}}, map[reflect.Type]bool{}
	for len(next) > 0 {
		current, count := next, map[reflect.Type]int{}
		next = nil
		for _, level := range current {
			count[level.typ]++
		}
		for _, level := range current {
			if visited[level.typ] {
				continue
			}
			visited[level.typ] = true
			for i := range level.typ.NumField() {
				field := level.typ.Field(i)
				fieldType := field.Type
				if fieldType.Name() == "" && fieldType.Kind() == reflect.Pointer {
					fieldType = fieldType.Elem()
				}
				tag := field.Tag.Get("json")
				if tag == "-" || !field.IsExported() && (!field.Anonymous || fieldType.Kind() != reflect.Struct) {
					continue
				}
				name, options, _ := strings.Cut(tag, ",")
				if !validJSONTagName(name) {
					name = ""
				}
				index := append(slices.Clone(level.index), i)
				if name == "" && field.Anonymous && fieldType.Kind() == reflect.Struct {
					next = append(next, embedded{fieldType, index})
					continue
				}
				resolved := jsonField{name: name, index: index, tagged: name != ""}
				if name == "" {
					resolved.name = field.Name
				}
				for option := range strings.SplitSeq(options, ",") {
					switch option {
					case "omitempty":
						resolved.omitEmpty = true
					case "omitzero":
						resolved.omitZero = true
					case "string":
						// encoding/json quotes only a bool, number or string.
						kind := fieldType.Kind()
						resolved.quoted = kind == reflect.Bool || kind == reflect.String || kind >= reflect.Int && kind <= reflect.Float64
					}
				}
				fields = append(fields, resolved)
				if count[level.typ] > 1 {
					// One type embedded twice at this depth claims the name twice, so neither copy survives.
					fields = append(fields, resolved)
				}
			}
		}
	}
	slices.SortFunc(fields, func(a, b jsonField) int {
		if order := cmp.Or(strings.Compare(a.name, b.name), cmp.Compare(len(a.index), len(b.index))); order != 0 {
			return order
		}
		if a.tagged != b.tagged {
			if a.tagged {
				return -1
			}
			return 1
		}
		return slices.Compare(a.index, b.index)
	})
	var dominant []jsonField
	for start, end := 0, 0; start < len(fields); start = end {
		for end = start + 1; end < len(fields) && fields[end].name == fields[start].name; end++ {
		}
		first := fields[start]
		if end-start == 1 || len(first.index) < len(fields[start+1].index) || first.tagged != fields[start+1].tagged {
			dominant = append(dominant, first)
		}
	}
	slices.SortFunc(dominant, func(a, b jsonField) int { return slices.Compare(a.index, b.index) })
	cached, _ := jsonFieldCache.LoadOrStore(typ, dominant)
	return cached.([]jsonField)
}

// Invalid names fall back to the Go field name, while their options still apply.
func validJSONTagName(name string) bool {
	for _, character := range name {
		if !strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", character) && !unicode.IsLetter(character) && !unicode.IsDigit(character) {
			return false
		}
	}
	return name != ""
}
