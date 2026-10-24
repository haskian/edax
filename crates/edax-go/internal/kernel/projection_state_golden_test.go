package kernel

import (
	"encoding/json"
	"testing"
)

func TestProjectionStateGolden(t *testing.T) {
	for _, row := range goldenRows[struct {
		ingestionGoldenResult
		Request json.RawMessage
		Records json.RawMessage
		Keys    json.RawMessage
		Lock    json.RawMessage
		Now     int64
	}](t, "projection_state") {
		t.Run(row.Name, func(t *testing.T) {
			reads, readErr := ProjectionReads(string(row.Request))
			if string(row.Keys) == "[]" || string(row.Lock) == "null" {
				if readErr == nil {
					t.Fatal("invalid request read keys accepted")
				}
			} else {
				expected, _ := json.Marshal(map[string]json.RawMessage{"keys": row.Keys, "lock": row.Lock})
				ingestionGoldenCheck(t, ingestionGoldenResult{Name: row.Name + "/reads", Want: expected}, json.RawMessage(reads), readErr)
			}
			result, err := ProjectionTransition(string(row.Request), string(row.Records), row.Now)
			ingestionGoldenCheck(t, row.ingestionGoldenResult, json.RawMessage(result), err)
		})
	}
}
