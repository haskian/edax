package native

import "C"

type Payload struct{ Name string }
type Ledger struct {
	label string
	ptr   C.Ledger
}

func GetLedger() *Ledger { return nil }
func (l *Ledger) Free()  {}
func (l *Ledger) Apply(first string, second string, note *string, rows []Payload) (*Payload, error) {
	return nil, nil
}
func GetVersions(available []uint32) uint32                   { return 0 }
func GetRecord(data []byte, labels map[string]string) Payload { return Payload{} }

func VersionsPresent(present bool, available []uint32) uint32 { return 0 }
