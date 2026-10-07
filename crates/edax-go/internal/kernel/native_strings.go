package kernel

import (
	"reflect"
	"strings"
	"unicode/utf8"
)

// C strings cannot carry an embedded NUL. Reject it at the conversion boundary
// instead of passing a different identifier to the kernel. Byte payloads retain
// their explicit length and can contain arbitrary bytes.
func checkNativeString(name, value string) error {
	if strings.IndexByte(value, 0) >= 0 {
		return &Error{Code: CodeCodec, Message: name + " contains a NUL byte that the native string boundary cannot represent"}
	}
	if !utf8.ValidString(value) {
		return &Error{Code: CodeCodec, Message: name + " is not valid UTF-8"}
	}
	return nil
}

// JSON encoders replace invalid UTF-8 with U+FFFD. A record field can also be
// returned later as a C string. Validate record text before either conversion.
func checkNativeValue(name string, value any) error {
	if !validNativeText(reflect.ValueOf(value)) {
		return &Error{Code: CodeCodec, Message: name + " contains a NUL byte or text that is not valid UTF-8"}
	}
	return nil
}
func validNativeText(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.String:
		return utf8.ValidString(value.String()) && strings.IndexByte(value.String(), 0) < 0
	case reflect.Pointer, reflect.Interface:
		return value.IsNil() || validNativeText(value.Elem())
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return true
		}
		for index := 0; index < value.Len(); index++ {
			if !validNativeText(value.Index(index)) {
				return false
			}
		}
	case reflect.Map:
		items := value.MapRange()
		for items.Next() {
			if !validNativeText(items.Key()) || !validNativeText(items.Value()) {
				return false
			}
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if !validNativeText(value.Field(index)) {
				return false
			}
		}
	}
	return true
}
