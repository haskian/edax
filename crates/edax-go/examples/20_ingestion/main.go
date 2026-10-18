// Reference the ingestion kernel through its in-repository Go bindings.
// Run from crates/edax-go: mise exec -- go run -race ./examples/20_ingestion
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

type order struct {
	ID     string `json:"order_id"`
	Status string `json:"status"`
}

type eventEnvelope struct {
	Stream   string `json:"aggregate_id"`
	EventID  string `json:"event_id"`
	Sequence uint64 `json:"sequence"`
	State    order  `json:"state"`
}

type input struct {
	Source   string        `json:"source"`
	Key      string        `json:"key"`
	Stream   string        `json:"stream"`
	EventID  string        `json:"event_id"`
	Sequence uint64        `json:"sequence"`
	Content  eventEnvelope `json:"content"`
	Value    *order        `json:"value"`
	Raw      string        `json:"raw"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: append replay rejected a conflict; deletion survived a stale upsert")
}

func run() (err error) {
	db := kernel.GetMemoryIngestion()
	defer func() { err = errors.Join(err, db.Close()) }()
	for _, mode := range []string{"append", "upsert"} {
		generation := "orders-" + mode + "-v1"
		manifest, err := json.Marshal(map[string]string{
			"mode": mode, "generation": generation, "source_incarnation": "orders-source-v1", "mapper_id": "order-state-v1",
		})
		if err != nil {
			return err
		}
		rowID := "order-42"
		if mode == "append" {
			rowID = "order-paid-1"
		}
		identity, err := json.Marshal(map[string]string{"tenant": "shop-a", "row_id": rowID})
		if err != nil {
			return err
		}
		key, err := kernel.IngestRowKey(string(manifest), string(identity))
		if err != nil {
			return err
		}
		originals := make([]input, 3)
		for i := range originals {
			source, err := kernel.IngestSourceKey(generation, "orders-source-v1", "orders", 0, int64(i))
			if err != nil {
				return err
			}
			state := order{ID: "order-42", Status: "paid"}
			originals[i] = input{Source: source, Key: key, Stream: "order-42", EventID: "order-paid-1", Sequence: 1, Content: eventEnvelope{Stream: "order-42", EventID: "order-paid-1", Sequence: 1, State: state}, Value: &state}
		}
		if mode == "append" {
			// One event ID at three source offsets: original, exact replay, changed content.
			originals[2].Content.State.Status = "cancelled"
			originals[2].Value = &order{ID: "order-42", Status: "cancelled"}
		} else {
			// A complete-state deletion at sequence 2 must survive the old paid state at 1.
			originals[1].EventID, originals[1].Sequence = "order-deleted-2", 2
			originals[1].Content.State.Status, originals[1].Value = "deleted", nil
			originals[1].Content.EventID, originals[1].Content.Sequence = "order-deleted-2", 2
		}
		for i := range originals {
			// Retain an example source envelope for rejection recovery.
			raw, err := json.Marshal(originals[i].Content)
			if err != nil {
				return err
			}
			originals[i].Raw = string(raw)
		}
		encoded, err := json.Marshal(originals)
		if err != nil {
			return err
		}
		want := []string{"accepted", "accepted", "accepted"}
		if mode == "append" {
			want[2] = "rejected"
		}
		for attempt := 0; attempt < 2; attempt++ {
			text, err := db.Commit(string(manifest), string(encoded))
			if err != nil {
				return err
			}
			var result struct {
				Outcomes []string `json:"outcomes"`
			}
			if err := json.Unmarshal([]byte(text), &result); err != nil {
				return err
			}
			if !reflect.DeepEqual(result.Outcomes, want) {
				return fmt.Errorf("%s outcomes: %v", mode, result.Outcomes)
			}
		}
		text, err := db.Get(key)
		if err != nil {
			return err
		}
		var stored struct {
			Stream   string `json:"stream"`
			Sequence uint64 `json:"sequence"`
			Value    *order `json:"value"`
		}
		if err := json.Unmarshal([]byte(text), &stored); err != nil {
			return err
		}
		if mode == "upsert" {
			if stored.Stream != "order-42" || stored.Sequence != 2 || stored.Value != nil {
				return fmt.Errorf("deletion marker lost: %s", text)
			}
		} else if stored.Value == nil || stored.Value.Status != "paid" {
			return fmt.Errorf("append changed: %s", text)
		}
		prefix, err := kernel.GenerationPrefix(generation)
		if err != nil {
			return err
		}
		pending, err := db.Pending(prefix, 100)
		if err != nil {
			return err
		}
		var rejections map[string]json.RawMessage
		if err := json.Unmarshal([]byte(pending), &rejections); err != nil {
			return err
		}
		wantPending := 0
		if mode == "append" {
			wantPending = 1
		}
		if len(rejections) != wantPending {
			return fmt.Errorf("%s pending rejections: %s", mode, pending)
		}
		fmt.Printf("%s: outcomes=%v, stored sequence=%d, pending rejections=%d\n", mode, want, stored.Sequence, len(rejections))
	}
	return nil
}
