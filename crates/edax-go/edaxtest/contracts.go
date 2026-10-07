// Package edaxtest checks backend implementations against the Go SDK contracts.
// Factories must return an empty, isolated backend and register cleanup with t.
package edaxtest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Factory returns an empty, isolated backend and registers resource cleanup with t.
type Factory[T any] func(testing.TB) T

// EventBackend keeps the event log and its transactional outbox together.
type EventBackend interface {
	edax.EventStore
	edax.Outbox[edax.EventMessage]
}

func ctx(t testing.TB) context.Context {
	t.Helper()
	deadlineCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return deadlineCtx
}
func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func draft(id string, sequence uint64) edax.EventDraft {
	return edax.EventDraft{ID: id, AggregateID: "invoice:1042", Sequence: &sequence, Name: "InvoiceIssued", Version: 1, Data: []byte(`{"invoice_id":"1042","total":"49.90","currency":"EUR"}`), CorrelationID: "checkout:1042", CausationID: "issue:1042"}
}
func command() edax.CommandMessage {
	return edax.CommandMessage{ID: "charge:1042", Name: "ChargeInvoice", Version: 1, Data: []byte(`{"invoice_id":"1042","total":"49.90","currency":"EUR"}`), CorrelationID: "checkout:1042", CausationID: "issued:1042", IssuedAt: 1760000000000}
}

func sagaCommand(t testing.TB) edax.CommandMessage {
	t.Helper()
	payload := command()
	result, err := kernel.SagaCommand("billing", "invoice:1042", 1, 0, edax.Payload{Name: payload.Name, Version: payload.Version, Data: payload.Data}, payload.CausationID, payload.CorrelationID, payload.IssuedAt, nil)
	check(t, err)
	return *result
}

// EventStore checks optimistic concurrency, batch atomicity, snapshots and outbox settlement.
func EventStore(t *testing.T, factory Factory[EventBackend]) {
	t.Helper()
	for _, name := range []string{"append_and_outbox", "stale_version", "duplicate_batch", "snapshots"} {
		t.Run(name, func(t *testing.T) {
			store := factory(t)
			ctx := ctx(t)
			first, err := store.Append(ctx, "invoice:1042", 0, []edax.EventDraft{draft("issued:1042", 1)})
			check(t, err)
			if len(first) != 1 || first[0].Sequence == nil || *first[0].Sequence != 1 {
				t.Fatalf("stored event: %+v", first)
			}
			switch name {
			case "append_and_outbox":
				loaded, err := store.Load(ctx, "invoice:1042", 0)
				check(t, err)
				if len(loaded) != 1 || loaded[0].ID != "issued:1042" || loaded[0].CorrelationID != "checkout:1042" {
					t.Fatalf("lost envelope fields: %+v", loaded)
				}
				after, err := store.Load(ctx, "invoice:1042", 1)
				check(t, err)
				if len(after) != 0 {
					t.Fatalf("after is not exclusive: %+v", after)
				}
				pending, err := store.Unpublished(ctx, 10)
				check(t, err)
				if len(pending) != 1 || pending[0].ID != loaded[0].ID {
					t.Fatalf("outbox: %+v", pending)
				}
				for range 2 {
					check(t, store.MarkPublished(ctx, []string{pending[0].ID}))
				}
				pending, err = store.Unpublished(ctx, 10)
				check(t, err)
				if len(pending) != 0 {
					t.Fatalf("published rows replayed: %+v", pending)
				}
			case "stale_version":
				_, err = store.Append(ctx, "invoice:1042", 0, []edax.EventDraft{draft("paid:1042", 2)})
				if !errors.Is(err, edax.ErrConcurrency) {
					t.Fatalf("stale append: %v", err)
				}
				rows, err := store.Load(ctx, "invoice:1042", 0)
				check(t, err)
				if len(rows) != 1 {
					t.Fatalf("failed append wrote rows: %+v", rows)
				}
			case "duplicate_batch":
				_, err = store.Append(ctx, "invoice:1042", 1, []edax.EventDraft{draft("paid:1042", 2), draft("issued:1042", 3)})
				if !errors.Is(err, edax.ErrCodec) {
					t.Fatalf("duplicate event id: %v", err)
				}
				rows, err := store.Load(ctx, "invoice:1042", 0)
				check(t, err)
				if len(rows) != 1 {
					t.Fatalf("batch partially written: %+v", rows)
				}
				pending, err := store.Unpublished(ctx, 10)
				check(t, err)
				if len(pending) != 1 {
					t.Fatalf("failed batch reached outbox: %+v", pending)
				}
			case "snapshots":
				snapshot := edax.Snapshot{AggregateID: "invoice:1042", TypeName: "Invoice", Version: 1, Data: []byte(`{"paid":false,"total":"49.90"}`)}
				check(t, store.SaveSnapshot(ctx, snapshot))
				loaded, err := store.LoadSnapshot(ctx, "invoice:1042")
				check(t, err)
				if loaded == nil || loaded.Version != 1 || string(loaded.Data) != string(snapshot.Data) {
					t.Fatalf("snapshot: %+v", loaded)
				}
				snapshot.Version = 2
				if err = store.SaveSnapshot(ctx, snapshot); !errors.Is(err, edax.ErrCodec) {
					t.Fatalf("snapshot ahead of stream: %v", err)
				}
			}
		})
	}
}

// IdempotencyStore checks exclusive claims, stale handles and settled replay.
func IdempotencyStore(t *testing.T, factory Factory[edax.IdempotencyStore]) {
	t.Helper()
	for _, name := range []string{"claim_and_replay", "stale_handle", "forget"} {
		t.Run(name, func(t *testing.T) {
			store := factory(t)
			ctx := ctx(t)
			key := "billing:charge:1042"
			first, err := store.Claim(ctx, key, 42)
			check(t, err)
			if !first.Won {
				t.Fatal("empty key was not claimed")
			}
			other, err := store.Claim(ctx, key, 42)
			check(t, err)
			if other.Won || other.Prior == nil {
				t.Fatalf("second claimant won: %+v", other)
			}
			result := `{"invoice_id":"1042","paid":true}`
			record := edax.IdempotencyRecord{PayloadHash: 42, State: "done", ResultJSON: &result}
			if name == "stale_handle" {
				check(t, store.Release(ctx, key, first.Handle))
				next, err := store.Claim(ctx, key, 42)
				check(t, err)
				if !next.Won {
					t.Fatal("released claim still held")
				}
				if err = store.Complete(ctx, key, first.Handle, record); err == nil {
					t.Fatal("stale complete succeeded")
				}
				check(t, store.Release(ctx, key, first.Handle))
				check(t, store.Complete(ctx, key, next.Handle, record))
				return
			}
			if name == "forget" {
				if err = store.Forget(ctx, key); !errors.Is(err, edax.ErrInProgress) {
					t.Fatalf("forget active claim: %v", err)
				}
			}
			check(t, store.Complete(ctx, key, first.Handle, record))
			check(t, store.Release(ctx, key, first.Handle))
			prior, err := store.Claim(ctx, key, 42)
			check(t, err)
			if prior.Won || prior.Prior == nil {
				t.Fatal("settled release removed cached record")
			}
			decoded, err := kernel.DecodeIdempotencyRecord(*prior.Prior)
			check(t, err)
			if decoded.State != "done" || decoded.PayloadHash != 42 || decoded.ResultJSON == nil {
				t.Fatalf("cached result: %+v", decoded)
			}
			if name == "forget" {
				check(t, store.Forget(ctx, key))
				next, err := store.Claim(ctx, key, 42)
				check(t, err)
				if !next.Won {
					t.Fatal("forgotten key did not admit a new claim")
				}
			}
		})
	}
}

// ProjectionStore checks optimistic versions, reads and query behavior for a backend.
func ProjectionStore(t *testing.T, factory Factory[edax.ProjectionStore]) {
	t.Helper()
	store := factory(t)
	ctx := ctx(t)
	absent, err := store.Get(ctx, "invoice:1042")
	check(t, err)
	if absent != nil {
		t.Fatal("missing projection is not nil")
	}
	document := `{"invoice_id":"1042","paid":false,"total":"49.90"}`
	version, err := store.Save(ctx, "invoice:1042", 0, document)
	check(t, err)
	if version != 1 {
		t.Fatalf("initial version %d", version)
	}
	_, err = store.Save(ctx, "invoice:1042", 0, `{"paid":true}`)
	if !errors.Is(err, edax.ErrConcurrency) {
		t.Fatalf("stale projection write: %v", err)
	}
	loaded, err := store.Get(ctx, "invoice:1042")
	check(t, err)
	if loaded == nil || loaded.Version != 1 {
		t.Fatalf("projection: %+v", loaded)
	}
	var parsed map[string]any
	check(t, json.Unmarshal([]byte(loaded.DocumentJSON), &parsed))
	if parsed["invoice_id"] != "1042" || parsed["paid"] != false {
		t.Fatalf("failed write changed projection: %+v", parsed)
	}
}

// CommandOutbox checks enqueue, ordered reads and idempotent publication marks.
func CommandOutbox(t *testing.T, factory Factory[edax.CommandOutbox]) {
	t.Helper()
	store := factory(t)
	ctx := ctx(t)
	message := command()
	check(t, edax.WithinTransaction(ctx, store.UnitOfWork(), func(ctx context.Context) error { return store.Enqueue(ctx, []edax.CommandMessage{message}) }))
	rows, err := store.Unpublished(ctx, 10)
	check(t, err)
	if len(rows) != 1 || rows[0].ID != message.ID {
		t.Fatalf("outbox: %+v", rows)
	}
	for range 2 {
		check(t, store.MarkPublished(ctx, []string{message.ID}))
	}
	rows, err = store.Unpublished(ctx, 10)
	check(t, err)
	if len(rows) != 0 {
		t.Fatalf("published commands still pending: %+v", rows)
	}
}

// SagaStore checks version conflicts, state, commands and completion tombstones.
func SagaStore(t *testing.T, factory Factory[edax.SagaStore]) {
	t.Helper()
	store := factory(t)
	ctx := ctx(t)
	missing, err := store.Load(ctx, "billing", "invoice:1042")
	check(t, err)
	if missing != nil {
		t.Fatal("missing saga is not nil")
	}
	version, err := store.Save(ctx, "billing", "invoice:1042", json.RawMessage(`{"phase":"charging"}`), 0, "{}", []edax.CommandMessage{sagaCommand(t)})
	check(t, err)
	if version != 1 {
		t.Fatalf("saga version %d", version)
	}
	_, err = store.Save(ctx, "billing", "invoice:1042", json.RawMessage(`{"phase":"wrong"}`), 0, "{}", nil)
	if !errors.Is(err, edax.ErrConcurrency) {
		t.Fatalf("stale saga write: %v", err)
	}
	instance, err := store.Load(ctx, "billing", "invoice:1042")
	check(t, err)
	if instance == nil || instance.Version != 1 || instance.Completed {
		t.Fatalf("saga instance: %+v", instance)
	}
	commands, err := store.Unpublished(ctx, "billing", 10)
	check(t, err)
	if len(commands) != 1 || commands[0].ID != sagaCommand(t).ID {
		t.Fatalf("saga outbox: %+v", commands)
	}
	_, err = store.Complete(ctx, "billing", "invoice:1042", version, nil)
	check(t, err)
	instance, err = store.Load(ctx, "billing", "invoice:1042")
	check(t, err)
	if instance == nil || !instance.Completed {
		t.Fatalf("missing completion tombstone: %+v", instance)
	}
	for range 2 {
		check(t, store.MarkPublished(ctx, "billing", []string{sagaCommand(t).ID}))
	}
	commands, err = store.Unpublished(ctx, "billing", 10)
	check(t, err)
	if len(commands) != 0 {
		t.Fatal("published saga command remained")
	}
}

// TransactionFixture's stores must share Owner and the same physical transaction.
type TransactionFixture struct {
	Owner       edax.UnitOfWork
	Events      EventBackend
	Ledger      edax.IdempotencyStore
	Commands    edax.CommandOutbox
	Projections edax.ProjectionStore
	Sagas       edax.SagaStore
}

// Transactions verifies atomic commit and rollback across all stores in each fresh fixture.
func Transactions(t *testing.T, factory Factory[TransactionFixture]) {
	t.Helper()
	for _, rollback := range []bool{false, true} {
		name := "commit"
		if rollback {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			fixture := factory(t)
			ctx := ctx(t)
			failure := errors.New("billing operation refused")
			err := edax.WithinTransaction(ctx, fixture.Owner, func(txctx context.Context) error {
				if _, err := fixture.Events.Append(txctx, "invoice:1042", 0, []edax.EventDraft{draft("issued:1042", 1)}); err != nil {
					return err
				}
				rows, err := fixture.Events.Load(txctx, "invoice:1042", 0)
				if err != nil {
					return err
				}
				if len(rows) != 1 {
					t.Fatal("transaction cannot read its writes")
				}
				claim, err := fixture.Ledger.Claim(txctx, "billing:charge:1042", 42)
				if err != nil {
					return err
				}
				if !claim.Won {
					t.Fatal("transaction lost empty claim")
				}
				result := `{"paid":true}`
				if err = fixture.Ledger.Complete(txctx, "billing:charge:1042", claim.Handle, edax.IdempotencyRecord{PayloadHash: 42, State: "done", ResultJSON: &result}); err != nil {
					return err
				}
				if err = fixture.Commands.Enqueue(txctx, []edax.CommandMessage{command()}); err != nil {
					return err
				}
				if _, err = fixture.Projections.Save(txctx, "invoice:1042", 0, `{"paid":true}`); err != nil {
					return err
				}
				if _, err = fixture.Sagas.Save(txctx, "billing", "invoice:1042", json.RawMessage(`{"phase":"charging"}`), 0, "{}", []edax.CommandMessage{sagaCommand(t)}); err != nil {
					return err
				}
				if rollback {
					return failure
				}
				return nil
			})
			if rollback {
				if !errors.Is(err, failure) {
					t.Fatalf("rollback failure: %v", err)
				}
			} else {
				check(t, err)
			}
			expected := 1
			if rollback {
				expected = 0
			}
			rows, err := fixture.Events.Load(ctx, "invoice:1042", 0)
			check(t, err)
			if len(rows) != expected {
				t.Fatalf("event atomicity: %+v", rows)
			}
			outbox, err := fixture.Events.Unpublished(ctx, 10)
			check(t, err)
			if len(outbox) != expected {
				t.Fatal("event outbox not atomic")
			}
			commands, err := fixture.Commands.Unpublished(ctx, 10)
			check(t, err)
			if len(commands) != expected {
				t.Fatal("command outbox not atomic")
			}
			projection, err := fixture.Projections.Get(ctx, "invoice:1042")
			check(t, err)
			if (projection == nil) != rollback {
				t.Fatal("projection not atomic")
			}
			saga, err := fixture.Sagas.Load(ctx, "billing", "invoice:1042")
			check(t, err)
			if (saga == nil) != rollback {
				t.Fatal("saga not atomic")
			}
			sagaCommands, err := fixture.Sagas.Unpublished(ctx, "billing", 10)
			check(t, err)
			if len(sagaCommands) != expected {
				t.Fatal("saga commands not atomic")
			}
			claim, err := fixture.Ledger.Claim(ctx, "billing:charge:1042", 42)
			check(t, err)
			if claim.Won != rollback {
				t.Fatal("ledger not atomic")
			}
		})
	}
}

// EventBrokerFixture must route these same-aggregate events to one ordered partition.
type EventBrokerFixture struct {
	Publish func(context.Context, edax.EventMessage) error
	Poll    func(context.Context) (*edax.Delivery[edax.EventMessage], error)
}

// EventBroker checks per-key ordering and broker redelivery after negative acknowledgement.
func EventBroker(t *testing.T, factory Factory[EventBrokerFixture]) {
	t.Helper()
	broker := factory(t)
	ctx := ctx(t)
	for i, id := range []string{"issued:1042", "paid:1042"} {
		event, err := kernel.RecordEvent(draft(id, uint64(i+1)), 1760000000000)
		check(t, err)
		check(t, broker.Publish(ctx, *event))
	}
	first, err := broker.Poll(ctx)
	check(t, err)
	second, err := broker.Poll(ctx)
	check(t, err)
	if first == nil || second == nil || first.Message.ID != "issued:1042" || second.Message.ID != "paid:1042" {
		t.Fatal("broker changed per-key order")
	}
	check(t, first.Nack(ctx))
	if second.IsLive() {
		t.Fatal("nack did not retire later delivery")
	}
	replay, err := broker.Poll(ctx)
	check(t, err)
	if replay == nil || replay.Message.ID != first.Message.ID {
		t.Fatal("nack did not replay first message")
	}
	check(t, replay.Ack(ctx))
	check(t, replay.Ack(ctx))
}
