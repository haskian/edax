// Generator whitespace and comments do not define the API.
package native

import "C"

type (
	Payload struct {
		Name string // public wire name
	}
	Ledger struct {
		label string
		// The handle need not be the first field.
		ptr C.Ledger
	}
)

func GetLedger() (result *Ledger) {
	return nil
}
func (owner *Ledger) Free() {}
func (owner *Ledger) Apply(
	first, second string, // grouped inputs are separate arguments
	note *string,
	rows []Payload,
) (result *Payload, failure error) {
	return nil, nil
}
func GetVersions(available []uint32) (version uint32) { return 0 }
func GetRecord(
	data []byte,
	labels map[string]string,
) (result Payload) {
	return Payload{}
}

func Ptr[T any](value T) *T { return &value }

func VersionsPresent(present bool, available []uint32) uint32 { return 0 }
