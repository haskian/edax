package kernel

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestKafkaIngestionRawGolden(t *testing.T) {
	for _, row := range ingestionGoldenRows[struct {
		ingestionGoldenResult
		Record        KafkaRecord
		Topic         string
		Partition     int32
		Offset        int64
		Limit         uint64
		Timestamp     *int64
		TimestampType *int32 `json:"timestamp_type"`
		Decoded       KafkaRecord
		Raw           json.RawMessage
		Source        string
	}](t, "kafka_ingestion_raw") {
		t.Run(row.Name, func(t *testing.T) {
			switch row.Op {
			case "encode":
				source, err := json.Marshal(map[string]any{"topic": row.Topic, "partition": row.Partition, "offset": row.Offset, "timestamp": row.Timestamp, "timestamp_type": row.TimestampType})
				if err != nil {
					t.Fatal(err)
				}
				raw, err := KafkaIngestionRaw(row.Record, string(source), row.Limit)
				ingestionGoldenCheck(t, row.ingestionGoldenResult, json.RawMessage(raw), err)
				decoded, err := KafkaIngestionRecord(raw)
				if err != nil {
					t.Fatal(err)
				}
				if decoded == nil {
					t.Fatal("decoded record is absent")
				}
				if !bytes.Equal(decoded.Key, row.Decoded.Key) || !bytes.Equal(decoded.Value, row.Decoded.Value) || len(decoded.Headers) != len(row.Decoded.Headers) {
					t.Fatalf("decoded record %+v, want %+v", decoded, row.Decoded)
				}
				for i, header := range decoded.Headers {
					want := row.Decoded.Headers[i]
					if header.Key != want.Key || !bytes.Equal(header.Value, want.Value) {
						t.Fatalf("decoded header %d: %+v, want %+v", i, header, want)
					}
				}
			case "decode":
				decoded, err := KafkaIngestionRecord(string(row.Raw))
				ingestionGoldenCheck(t, row.ingestionGoldenResult, decoded, err)
			case "rejection":
				decoded, err := KafkaIngestionRejectionRecord(string(row.Raw), row.Source)
				if row.Error != nil {
					ingestionGoldenCheck(t, row.ingestionGoldenResult, decoded, err)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				var want KafkaRecord
				if err := json.Unmarshal(row.Want, &want); err != nil {
					t.Fatal(err)
				}
				if decoded == nil || !bytes.Equal(decoded.Key, want.Key) || !bytes.Equal(decoded.Value, want.Value) || len(decoded.Headers) != len(want.Headers) {
					t.Fatalf("rejection record %+v, want %+v", decoded, want)
				}
				for i, header := range decoded.Headers {
					if header.Key != want.Headers[i].Key || !bytes.Equal(header.Value, want.Headers[i].Value) {
						t.Fatalf("rejection header %d: %+v, want %+v", i, header, want.Headers[i])
					}
				}
			default:
				t.Fatalf("unknown raw ingestion operation %q", row.Op)
			}
		})
	}
}
