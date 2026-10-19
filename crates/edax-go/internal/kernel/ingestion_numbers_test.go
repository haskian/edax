package kernel

import (
	"encoding/json"
	"testing"
)

func TestIngestionRawNumbers(t *testing.T) {
	for _, row := range ingestionGoldenRows[struct {
		ingestionGoldenResult
		Envelope    json.RawMessage
		EventJSON   string `json:"event_json"`
		Mode        string
		InputsJSON  string `json:"inputs_json"`
		RequestJSON string `json:"request_json"`
	}](t, "ingestion_numbers") {
		t.Run(row.Name, func(t *testing.T) {
			var got string
			var err error
			switch row.Op {
			case "plan":
				got, err = IngestPlan(row.Mode, row.InputsJSON, "{}", "{}")
			case "projection_sealed_input":
				got, err = ProjectionSealedInput(9223372036854775807, string(row.Envelope), &row.EventJSON)
			case "projection_keys":
				got, err = ProjectionKeys(row.RequestJSON)
			default:
				t.Fatalf("unknown raw numeric operation %q", row.Op)
			}
			ingestionGoldenCheck(t, row.ingestionGoldenResult, json.RawMessage(got), err)
		})
	}
}
