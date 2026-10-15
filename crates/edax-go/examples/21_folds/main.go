// Exercise ordered fold decisions using the in-repository Go kernel bindings.
// This single-process fixture uses synthetic source evidence and a fixed clock.
// Its map has no durability or locking.
// Run from crates/edax-go: mise exec -- go run -race ./examples/21_folds
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

const contract = `{"generation":"balances-v1","source_identity":"accounts-source-v1","source_history":"no-truncation-v1","topic":"accounts","reducer_id":"balance-v1","envelope_id":"event-v1","keying_id":"account-v1","effect_namespace":"receipts-v1","starts":{"0":0}}`
const proof = `{"identity":"accounts-source-v1","history":"no-truncation-v1","beginning":0,"end":4}`

type state struct {
	BalanceCents int64 `json:"balance_cents"`
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

// transition stores exactly the writes the kernel returns. A production adapter must
// read locked records and commit every write in one transaction before returning.
func transition(records map[string]json.RawMessage, operation map[string]any) (json.RawMessage, error) {
	request, err := json.Marshal(map[string]any{"contract": json.RawMessage(contract), "operation": operation})
	if err != nil {
		return nil, err
	}
	stored, err := json.Marshal(records)
	if err != nil {
		return nil, err
	}
	text, err := kernel.FoldTransition(string(request), string(stored), 100)
	if err != nil {
		return nil, err
	}
	var plan struct {
		Writes map[string]json.RawMessage `json:"writes"`
		Result json.RawMessage            `json:"result"`
	}
	if err := json.Unmarshal([]byte(text), &plan); err != nil {
		return nil, err
	}
	for key, value := range plan.Writes {
		records[key] = value
	}
	return plan.Result, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: two deltas produced 950 cents; replay skipped the reducer; sequence gap refused")
}

func run() error {
	records := map[string]json.RawMessage{}
	if _, err := transition(records, map[string]any{"op": "initialize"}); err != nil {
		return err
	}
	lease, err := transition(records, map[string]any{"op": "acquire", "partition": 0, "owner": "worker-a", "lease_ms": 1000, "proof": json.RawMessage(proof)})
	if err != nil {
		return err
	}
	first := event{Tenant: "bank-eu", Aggregate: "account-42", EventID: "credit-1", Sequence: 1, EventType: "BalanceChanged", SchemaVersion: "1", Payload: delta{1250}, Metadata: map[string]string{"correlation_id": "payment-99"}}
	second := first
	second.EventID, second.Sequence, second.Payload = "debit-2", 2, delta{-300}
	calls := 0
	for offset, incoming := range []event{first, first, second} {
		op := map[string]any{"op": "prepare", "lease": lease, "offset": offset, "event": incoming, "proof": json.RawMessage(proof)}
		text, err := transition(records, op)
		if err != nil {
			return err
		}
		var prepared struct {
			Novel bool   `json:"novel"`
			State *state `json:"state"`
			Event event  `json:"event"`
		}
		if err := json.Unmarshal(text, &prepared); err != nil {
			return err
		}
		var next state
		if prepared.Novel {
			calls++
			next = reduceBalance(prepared.State, prepared.Event)
		}
		// Apply also advances the source cursor for duplicates; their reduced value is ignored.
		op["op"], op["event"] = "apply", prepared.Event
		op["reduced"] = map[string]any{"state": next, "intents": []any{}}
		lease, err = transition(records, op)
		if err != nil {
			return err
		}
		fmt.Printf("offset %d: novel=%t, reducer calls=%d\n", offset, prepared.Novel, calls)
	}
	text, err := transition(records, map[string]any{"op": "get", "tenant": "bank-eu", "aggregate": "account-42"})
	if err != nil {
		return err
	}
	var stored struct {
		Sequence int64 `json:"sequence"`
		Value    state `json:"value"`
	}
	if err := json.Unmarshal(text, &stored); err != nil {
		return err
	}
	if calls != 2 || stored.Value.BalanceCents != 950 || stored.Sequence != 2 {
		return fmt.Errorf("unexpected state %s after %d reducer calls", text, calls)
	}
	gap := second
	gap.Sequence, gap.EventID = 4, "credit-4"
	if _, err := transition(records, map[string]any{"op": "prepare", "lease": lease, "offset": 3, "event": gap, "proof": json.RawMessage(proof)}); err == nil || err.Error() != "codec: fold aggregate sequence gap" {
		return fmt.Errorf("expected sequence gap refusal, got %v", err)
	}
	return nil
}
