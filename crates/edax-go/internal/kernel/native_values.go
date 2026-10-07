package kernel

import "reflect"

// Go treats nil and empty collections alike. Rust's required Vec and map fields
// accept arrays and objects, not JSON null. Copy input containers before replacing
// their nil values so a native call never changes the caller's message.
func normalizeNative[T any](value T) T {
	return normalizeNativeValue(reflect.ValueOf(value)).Interface().(T)
}

func normalizeNativeValue(value reflect.Value) reflect.Value {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return value
		}
		cloned := reflect.New(value.Type().Elem())
		cloned.Elem().Set(normalizeNativeValue(value.Elem()))
		return cloned
	case reflect.Slice:
		cloned := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := 0; index < value.Len(); index++ {
			cloned.Index(index).Set(normalizeNativeValue(value.Index(index)))
		}
		return cloned
	case reflect.Map:
		cloned := reflect.MakeMapWithSize(value.Type(), value.Len())
		values := value.MapRange()
		for values.Next() {
			cloned.SetMapIndex(values.Key(), normalizeNativeValue(values.Value()))
		}
		return cloned
	case reflect.Struct:
		cloned := reflect.New(value.Type()).Elem()
		cloned.Set(value)
		for index := 0; index < value.NumField(); index++ {
			if cloned.Field(index).CanSet() {
				cloned.Field(index).Set(normalizeNativeValue(value.Field(index)))
			}
		}
		return cloned
	default:
		return value
	}
}

func NewMemoryTransactor() *MemoryTransactor { return GetMemoryTransactor() }
func NewMemoryBroker(partitions uint32, tenancy string) (*MemoryBroker, error) {
	return GetMemoryBroker(partitions, tenancy)
}
func NewRetries(maxAttempts uint32, sink bool) *Retries { return GetRetries(maxAttempts, sink) }
func NewDrain(maxAttempts uint32, sink bool) *Drain     { return GetDrain(maxAttempts, sink) }
func NewOffsetTracker() *OffsetTracker                  { return GetOffsetTracker() }
func NewStreak() *Streak                                { return GetStreak() }
