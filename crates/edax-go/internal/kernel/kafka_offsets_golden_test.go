package kernel

import (
	"encoding/json"
	"testing"
)

// Replay the entire offset protocol, including late commits from a revoked generation.
func TestKafkaOffsetsGolden(t *testing.T) {
	for _, scenario := range goldenRows[struct {
		Name  string
		Steps []struct {
			Op        string
			Topic     string
			Partition int32
			Offset    int64
			Token     int64
			Slot      int
			Want      json.RawMessage
		}
	}](t, "kafka_offsets") {
		t.Run(scenario.Name, func(t *testing.T) {
			tracker := GetOffsetTracker()
			defer tracker.Close()
			captured := map[int]KafkaDue{}
			dueValue := func(d KafkaDue) map[string]any {
				return map[string]any{"topic": d.Topic, "partition": d.Partition, "offset": d.Offset}
			}
			for _, step := range scenario.Steps {
				var got any = map[string]any{}
				var err error
				switch step.Op {
				case "book":
					var token int64
					token, err = tracker.Book(step.Topic, step.Partition, step.Offset)
					got = map[string]any{"token": token}
					if err != nil {
						got = map[string]any{"error": err.Error()}
						err = nil
					}
				case "ack":
					var settled bool
					settled, err = tracker.Ack(step.Token)
					decision := "stale"
					if settled {
						decision = "settled"
					}
					got = map[string]any{"decision": decision}
				case "nack":
					var decision *NackDecision
					decision, err = tracker.Nack(step.Token)
					got = map[string]any{"decision": "stale"}
					if decision != nil && decision.Seek != nil {
						got = map[string]any{"decision": "seek", "offset": *decision.Seek}
					}
				case "is_live":
					var live bool
					live, err = tracker.IsLive(step.Token)
					got = map[string]any{"live": live}
				case "due", "capture_due":
					var due []KafkaDue
					due, err = tracker.Due()
					if step.Op == "capture_due" {
						if len(due) != 1 {
							t.Fatalf("capture_due: %v", due)
						}
						captured[step.Slot] = due[0]
						got = map[string]any{"due": dueValue(due[0])}
					} else {
						rows := []any{}
						for _, d := range due {
							rows = append(rows, dueValue(d))
						}
						got = map[string]any{"rows": rows}
					}
				case "committed":
					err = tracker.Committed(step.Topic, step.Partition, step.Offset)
				case "committed_generation":
					due, ok := captured[step.Slot]
					if !ok {
						t.Fatalf("uncaptured slot %d", step.Slot)
					}
					err = tracker.CommittedGeneration(due.Topic, due.Partition, due.Offset, due.Generation)
				case "reset":
					err = tracker.Reset(step.Topic, step.Partition)
				case "revoke":
					var due *KafkaDue
					due, err = tracker.Revoke(step.Topic, step.Partition)
					var value any
					if due != nil {
						value = dueValue(*due)
					}
					got = map[string]any{"due": value}
				case "stats":
					var stats []PartitionStats
					stats, err = tracker.Stats()
					rows := []any{}
					for _, s := range stats {
						rows = append(rows, map[string]any{"topic": s.Topic, "partition": s.Partition, "unsettled": s.Unsettled, "live": s.Live, "max_delivered": s.MaxDelivered, "committed": s.Committed})
					}
					got = map[string]any{"rows": rows}
				default:
					t.Fatalf("unknown offset operation %q", step.Op)
				}
				ingestionGoldenCheck(t, ingestionGoldenResult{Name: scenario.Name + "/" + step.Op, Want: step.Want}, got, err)
			}
		})
	}
}
