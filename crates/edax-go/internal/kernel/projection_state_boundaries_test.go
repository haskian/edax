package kernel

import (
	"encoding/json"
	"testing"
)

func TestProjectionStateBoundariesGolden(t *testing.T) {
	for _, fixture := range []string{"projection_state_boundaries", "postgres_projection_state"} {
		for _, row := range ingestionGoldenRows[struct {
			ingestionGoldenResult
			Request  json.RawMessage
			Contract json.RawMessage
			Proof    json.RawMessage
			Cursor   int64
			Manifest json.RawMessage
			Intent   json.RawMessage
			Config   json.RawMessage
		}](t, fixture) {
			t.Run(fixture+"/"+row.Name, func(t *testing.T) {
				var got any
				var err error
				switch row.Op {
				case "keys":
					var value string
					value, err = ProjectionKeys(string(row.Request))
					got = json.RawMessage(value)
				case "lock":
					var value string
					value, err = ProjectionReads(string(row.Request))
					if err == nil {
						var reads struct {
							Lock json.RawMessage `json:"lock"`
						}
						err = json.Unmarshal([]byte(value), &reads)
						got = reads.Lock
					}
				case "source":
					err = ProjectionCheckSource(string(row.Contract), string(row.Proof), row.Cursor)
				case "eligible":
					got, err = ProjectionIntentEligible(string(row.Manifest), string(row.Intent))
				case "sql":
					var value string
					value, err = PostgresProjectionStatements(string(row.Config))
					got = json.RawMessage(value)
				default:
					t.Fatalf("unknown projection boundary operation: %q", row.Op)
				}
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, err)
			})
		}
	}
}
