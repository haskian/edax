package kernel

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
)

func TestPartitionAdmissionGolden(t *testing.T) {
	type row struct {
		Contract, Inputs, Want json.RawMessage
		Floor                  string
	}
	for _, item := range goldenRows[row](t, "ingestion_partition") {
		floor, err := strconv.ParseInt(item.Floor, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := IngestTargetPlan(string(item.Contract), string(item.Inputs), "{}", "{}", floor)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ingestionGoldenValue(t, []byte(actual)), ingestionGoldenValue(t, item.Want)) {
			t.Fatalf("admission differs: %s", actual)
		}
	}
}

func TestPartitionSQLGolden(t *testing.T) {
	type row struct{ Config, Want json.RawMessage }
	for _, item := range goldenRows[row](t, "postgres_ingest_partition") {
		actual, err := PostgresIngestStatements(string(item.Config))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ingestionGoldenValue(t, []byte(actual)), ingestionGoldenValue(t, item.Want)) {
			t.Fatal("partition SQL differs")
		}
	}
}
