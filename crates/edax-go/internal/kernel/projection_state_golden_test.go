package kernel

import (
	"encoding/json"
	"testing"
)

func TestProjectionStateGolden(t *testing.T) {
	for _, row := range ingestionGoldenRows[struct {
		ingestionGoldenResult
		Request json.RawMessage
		Records json.RawMessage
		Now     int64
	}](t, "projection_state") {
		t.Run(row.Name, func(t *testing.T) {
			result, err := ProjectionTransition(string(row.Request), string(row.Records), row.Now)
			ingestionGoldenCheck(t, row.ingestionGoldenResult, json.RawMessage(result), err)
		})
	}
}
