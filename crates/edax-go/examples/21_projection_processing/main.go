// Exercise ordered projection state decisions using the in-repository Go kernel bindings.
// This single-process fixture uses synthetic source evidence, a fixed clock and the kernel's
// in-memory projection database, which has no durability.
// Run from crates/edax-go: mise exec -- go run -race ./examples/21_projection_processing
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

const contract = `{"generation":"balances-v1","source_identity":"accounts-source-v1","source_history":"no-truncation-v1","topic":"accounts","reducer_id":"balance-v1","envelope_id":"event-v1","keying_id":"account-v1","effect_namespace":"receipts-v1","starts":{"0":0},"at_rest":"sealed"}`
const proof = `{"identity":"accounts-source-v1","history":"no-truncation-v1","beginning":0,"end":4}`

type state struct {
	BalanceCents int64 `json:"balance_cents"`
}

// storedState is a state record as the kernel keeps it: its sequence and its two parts.
type storedState struct {
	Sequence int64  `json:"sequence"`
	Public   *state `json:"public"`
}
type delta struct {
	AmountCents int64 `json:"amount_cents"`
}
type event struct {
	Tenant        string            `json:"tenant"`
	Aggregate     string            `json:"aggregate"`
	EventID       string            `json:"event_id"`
	Sequence      int64             `json:"sequence"`
	EventType     string            `json:"event_type"`
	SchemaVersion string            `json:"schema_version"`
	Payload       delta             `json:"payload"`
	Metadata      map[string]string `json:"metadata"`
}

// reduceBalance is pure application logic. Identity and ordering belong to the kernel.
func reduceBalance(previous *state, incoming event) state {
	var next state
	if previous != nil {
		next = *previous
	}
	next.BalanceCents += incoming.Payload.AmountCents
	return next
}

// transition runs one operation as one transaction: it reads the records the kernel names, lets the
// kernel decide, and commits exactly the writes it returns. The commit refuses if a record it read
// changed since, so a racing transaction retries instead of overwriting.
func transition(db *kernel.MemoryProjectionDatabase, operation map[string]any) (json.RawMessage, error) {
	return transaction(db, operation, nil)
}

// Prepare and apply share one read-version snapshot. A concurrent cleanup or writer invalidates
// the final commit, so retry must rerun preparation and the pure reducer together.
func transaction(db *kernel.MemoryProjectionDatabase, operation map[string]any, reduce func(json.RawMessage) (map[string]any, error)) (json.RawMessage, error) {
	request, err := json.Marshal(map[string]any{"contract": json.RawMessage(contract), "operation": operation})
	if err != nil {
		return nil, err
	}
	keys, err := kernel.ProjectionKeys(string(request))
	if err != nil {
		return nil, err
	}
	text, err := db.Read(keys)
	if err != nil {
		return nil, err
	}
	var read struct {
		Records  json.RawMessage `json:"records"`
		Versions json.RawMessage `json:"versions"`
	}
	if err := json.Unmarshal([]byte(text), &read); err != nil {
		return nil, err
	}
	text, err = kernel.ProjectionTransition(string(request), string(read.Records), 100)
	if err != nil {
		return nil, err
	}
	var plan struct {
		Inserts json.RawMessage `json:"inserts"`
		Updates json.RawMessage `json:"updates"`
		Intents json.RawMessage `json:"intents"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal([]byte(text), &plan); err != nil {
		return nil, err
	}
	if reduce != nil {
		next, reduceErr := reduce(plan.Result)
		if reduceErr != nil {
			return nil, reduceErr
		}
		request, err = json.Marshal(map[string]any{"contract": json.RawMessage(contract), "operation": next})
		if err != nil {
			return nil, err
		}
		text, err = kernel.ProjectionTransition(string(request), string(read.Records), 100)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(text), &plan); err != nil {
			return nil, err
		}
	}
	writes, err := json.Marshal(map[string]json.RawMessage{"inserts": plan.Inserts, "updates": plan.Updates, "intents": plan.Intents})
	if err != nil {
		return nil, err
	}
	if err := db.Commit(string(read.Versions), string(writes)); err != nil {
		return nil, err
	}
	return plan.Result, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: two deltas produced 950 cents; replay skipped the reducer; sequence gap refused")
}

func run() (err error) {
	db := kernel.GetMemoryProjectionDatabase()
	defer func() { err = errors.Join(err, db.Close()) }()
	if _, err := transition(db, map[string]any{"op": "initialize"}); err != nil {
		return err
	}
	lease, err := transition(db, map[string]any{"op": "acquire", "partition": 0, "owner": "worker-a", "lease_ms": 1000, "proof": json.RawMessage(proof)})
	if err != nil {
		return err
	}
	first := event{Tenant: "bank-eu", Aggregate: "account-42", EventID: "credit-1", Sequence: 1, EventType: "BalanceChanged", SchemaVersion: "1", Payload: delta{1250}, Metadata: map[string]string{"correlation_id": "payment-99"}}
	second := first
	second.EventID, second.Sequence, second.Payload = "debit-2", 2, delta{-300}
	calls := 0
	// One batch, as a runner commits it: a delta, its replay at a later offset, and the next delta.
	inputs := []map[string]any{
		{"offset": 0, "event": first},
		{"offset": 1, "event": first},
		{"offset": 2, "event": second},
	}
	lease, err = transaction(db, map[string]any{"op": "prepare_batch", "lease": lease, "inputs": inputs, "proof": json.RawMessage(proof)}, func(text json.RawMessage) (map[string]any, error) {
		var prepared struct {
			Items []struct {
				Novel bool         `json:"novel"`
				State *storedState `json:"state"`
				Event event        `json:"event"`
			} `json:"items"`
		}
		if err := json.Unmarshal(text, &prepared); err != nil {
			return nil, err
		}
		// The reducer runs once per novel event; the host carries each aggregate's state between deltas.
		// State is per tenant and aggregate, as the kernel keys it.
		type aggregateKey struct{ tenant, aggregate string }
		current := map[aggregateKey]*state{}
		reduced := make([]any, len(prepared.Items))
		for i, item := range prepared.Items {
			if item.Novel {
				calls++
				key := aggregateKey{item.Event.Tenant, item.Event.Aggregate}
				previous, seen := current[key]
				if !seen && item.State != nil {
					previous = item.State.Public
				}
				next := reduceBalance(previous, item.Event)
				current[key] = &next
				// A balance names no person: it is public, queryable state with no private part.
				reduced[i] = map[string]any{"private": nil, "public": next, "intents": []any{}}
			}
			fmt.Printf("offset %d: novel=%t, reducer calls=%d\n", i, item.Novel, calls)
		}
		return map[string]any{"op": "apply_batch", "lease": lease, "inputs": inputs, "proof": json.RawMessage(proof), "reduced": reduced}, nil
	})
	if err != nil {
		return err
	}
	text, err := transition(db, map[string]any{"op": "get", "tenant": "bank-eu", "aggregate": "account-42"})
	if err != nil {
		return err
	}
	var stored storedState
	if err := json.Unmarshal(text, &stored); err != nil {
		return err
	}
	if calls != 2 || stored.Public == nil || stored.Public.BalanceCents != 950 || stored.Sequence != 2 {
		return fmt.Errorf("unexpected state %s after %d reducer calls", text, calls)
	}
	gap := second
	gap.Sequence, gap.EventID = 4, "credit-4"
	gapBatch := []map[string]any{{"offset": 3, "event": gap}}
	if _, err := transition(db, map[string]any{"op": "prepare_batch", "lease": lease, "inputs": gapBatch, "proof": json.RawMessage(proof)}); err == nil || err.Error() != "codec: projection aggregate sequence gap" {
		return fmt.Errorf("expected sequence gap refusal, got %v", err)
	}
	return nil
}
