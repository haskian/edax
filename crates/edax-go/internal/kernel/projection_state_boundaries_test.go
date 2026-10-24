package kernel

import (
	"encoding/json"
	"testing"
)

func TestProjectionStateBoundariesGolden(t *testing.T) {
	for _, fixture := range []string{"projection_state_boundaries", "postgres_projection_state"} {
		for _, row := range goldenRows[struct {
			ingestionGoldenResult
			Request    json.RawMessage
			Contract   json.RawMessage
			Proof      json.RawMessage
			Cursor     int64
			Manifest   json.RawMessage
			Intent     json.RawMessage
			Config     json.RawMessage
			Offset     int64
			Envelope   json.RawMessage
			Event      json.RawMessage
			Tenant     *string
			Subject    *string
			Reductions json.RawMessage
			Record     json.RawMessage
			Aggregate  string
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
				case "sealed_input":
					var event *string
					if text := string(row.Event); text != "null" {
						event = &text
					}
					var value string
					value, err = ProjectionSealedInput(row.Offset, string(row.Envelope), event)
					got = json.RawMessage(value)
				case "state_scope_range":
					var value string
					value, err = ProjectionStateScopeRange(string(row.Contract), row.Subject, row.Tenant)
					got = json.RawMessage(value)
				case "seal_plan":
					var value string
					value, err = ProjectionSealPlan(string(row.Contract), string(row.Reductions))
					got = json.RawMessage(value)
				case "open_state":
					var value string
					value, err = ProjectionOpenState(string(row.Contract), string(row.Record), row.Aggregate, row.Subject, row.Tenant)
					got = json.RawMessage(value)
				case "open_intent":
					var value string
					value, err = ProjectionOpenIntent(string(row.Contract), string(row.Record))
					got = json.RawMessage(value)
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
