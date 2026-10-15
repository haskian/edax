package kernel

import (
	"encoding/json"
	"testing"
)

func TestFoldBoundariesGolden(t *testing.T) {
	for _, fixture := range []string{"fold_boundaries", "postgres_fold"} {
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
					value, err = FoldKeys(string(row.Request))
					got = json.RawMessage(value)
				case "source":
					err = FoldCheckSource(string(row.Contract), string(row.Proof), row.Cursor)
				case "eligible":
					got, err = FoldIntentEligible(string(row.Manifest), string(row.Intent))
				case "sql":
					var value string
					value, err = PostgresFoldStatements(string(row.Config))
					got = json.RawMessage(value)
				default:
					t.Fatalf("unknown fold boundary operation: %q", row.Op)
				}
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, err)
			})
		}
	}
}
