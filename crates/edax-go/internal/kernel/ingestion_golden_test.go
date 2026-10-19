package kernel

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

type ingestionGoldenResult struct {
	Name  string
	Op    string
	Want  json.RawMessage
	Error *string
}

func ingestionGoldenRows[T any](t *testing.T, name string) []T {
	t.Helper()
	raw, err := os.ReadFile("../../../../testdata/golden/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []T
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatalf("empty golden fixture %s", name)
	}
	return rows
}

func ingestionGoldenValue(t *testing.T, raw []byte) any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func ingestionGoldenCheck(t *testing.T, row ingestionGoldenResult, got any, err error) {
	t.Helper()
	if row.Error != nil {
		if err == nil || err.Error() != *row.Error {
			t.Fatalf("error %v, want %q", err, *row.Error)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	raw, marshalErr := json.Marshal(got)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	// RawMessage preserves native JSON; UseNumber prevents rounding at 2^53.
	actual := ingestionGoldenValue(t, raw)
	want := ingestionGoldenValue(t, row.Want)
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("got %s\nwant %s", raw, row.Want)
	}
}

func TestBatchIngestionGolden(t *testing.T) {
	for _, row := range ingestionGoldenRows[struct {
		ingestionGoldenResult
		Set         string
		Config      json.RawMessage
		Records     uint64
		Bytes       uint64
		AgeMS       uint64 `json:"age_ms"`
		UsedRecords uint64 `json:"used_records"`
		UsedBytes   uint64 `json:"used_bytes"`
		Code        *string
		Attempt     uint32
		Waits       uint32
		Count       uint64
		MaxAttempts uint32 `json:"max_attempts"`
		RolledBack  bool   `json:"rolled_back"`
		Isolable    bool
	}](t, "event_batch") {
		t.Run(row.Name, func(t *testing.T) {
			switch row.Op {
			case "names":
				var got []string
				switch row.Set {
				case "batch_action":
					got = BatchActionNames()
				default:
					t.Fatalf("unknown batch names %q", row.Set)
				}
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, nil)
			case "config":
				got, err := BatchConfig(string(row.Config))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, json.RawMessage(got), err)
			case "cut":
				got, err := BatchCut(row.Records, row.Bytes, row.AgeMS, string(row.Config))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, err)
			case "admit":
				got, err := BatchAdmit(row.UsedRecords, row.UsedBytes, row.Records, row.Bytes, string(row.Config))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, err)
			case "reserve":
				got, err := BatchReserve(string(row.Config))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, err)
			case "charge":
				got, err := BatchCharge(row.Bytes, string(row.Config))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, err)
			case "recover":
				got, err := BatchRecover(row.Attempt, row.Waits, row.Count, row.MaxAttempts, row.RolledBack, row.Isolable, row.Code)
				ingestionGoldenCheck(t, row.ingestionGoldenResult, json.RawMessage(got), err)
			default:
				t.Fatalf("unknown batch operation %q", row.Op)
			}
		})
	}
}

func TestIngestionGolden(t *testing.T) {
	for _, row := range ingestionGoldenRows[struct {
		ingestionGoldenResult
		Set         string
		Mode        string
		Inputs      json.RawMessage
		Stored      json.RawMessage
		Rejected    json.RawMessage
		Previous    json.RawMessage
		Expected    json.RawMessage
		Contract    json.RawMessage
		Tenant      *string
		RowID       string `json:"row_id"`
		Target      string
		Incarnation string
		Topic       string
		Partition   int32
		Offset      int64
	}](t, "ingest") {
		t.Run(row.Name, func(t *testing.T) {
			switch row.Op {
			case "names":
				var got []string
				switch row.Set {
				case "ingest_mode":
					got = IngestModeNames()
				case "ingest_outcome":
					got = IngestOutcomeNames()
				default:
					t.Fatalf("unknown ingestion names %q", row.Set)
				}
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, nil)
			case "plan":
				got, err := IngestPlan(row.Mode, string(row.Inputs), string(row.Stored), string(row.Rejected))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, json.RawMessage(got), err)
			case "bind":
				err := IngestBindContract(string(row.Previous), string(row.Expected))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, true, err)
			case "validate":
				err := IngestValidateInputs(string(row.Contract), string(row.Inputs))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, true, err)
			case "row_key":
				manifest, err := json.Marshal(map[string]string{"generation": row.Target, "mode": row.Mode, "mapper_id": "test-mapper", "source_incarnation": "test-source"})
				if err != nil {
					t.Fatal(err)
				}
				identity, err := json.Marshal(map[string]any{"row_id": row.RowID, "tenant": row.Tenant})
				if err != nil {
					t.Fatal(err)
				}
				got, err := IngestRowKey(string(manifest), string(identity))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, err)
			case "source":
				got, err := IngestSourceKey(row.Target, row.Incarnation, row.Topic, row.Partition, row.Offset)
				ingestionGoldenCheck(t, row.ingestionGoldenResult, got, err)
			default:
				t.Fatalf("unknown ingestion operation %q", row.Op)
			}
		})
	}
}

func TestPostgresIngestionGolden(t *testing.T) {
	for _, row := range ingestionGoldenRows[struct {
		ingestionGoldenResult
		Config string
	}](t, "postgres_ingest") {
		t.Run(row.Name, func(t *testing.T) {
			switch row.Op {
			case "statements":
				got, err := PostgresIngestStatements(row.Config)
				ingestionGoldenCheck(t, row.ingestionGoldenResult, json.RawMessage(got), err)
			default:
				t.Fatalf("unknown postgres ingestion operation %q", row.Op)
			}
		})
	}
}
