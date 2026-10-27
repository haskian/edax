package edax_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

type invoiceIssued struct {
	Total      int    `json:"total"`
	CustomerID string `json:"customer_id"`
}

func TestUnsealedSiloRepositoryLoadsWithoutStoredTenant(t *testing.T) {
	for _, snapshotEvery := range []uint64{0, 1} {
		name := "events"
		if snapshotEvery != 0 {
			name = "snapshot"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := memory.New()
			defer db.Close()
			store, err := db.EventStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return new(invoiceIssued) }})
			if err != nil {
				t.Fatal(err)
			}
			repo, err := edax.NewRepository[*invoice](store, codec, invoiceCodec{}, edax.RepositoryOptions{Tenancy: edax.TenancySilo, SnapshotEvery: &snapshotEvery})
			if err != nil {
				t.Fatal(err)
			}
			aggregate, err := repo.Load(ctx, "invoice-42", edax.Ptr("tenant-acme"))
			if err != nil {
				t.Fatal(err)
			}
			if err := aggregate.SetSubject(edax.Ptr("customer-17")); err != nil {
				t.Fatal(err)
			}
			if err := edax.Apply(aggregate, &invoiceIssued{Total: 1250, CustomerID: "customer-17"}); err != nil {
				t.Fatal(err)
			}
			rows, err := repo.Save(ctx, aggregate, edax.SaveOptions{})
			if err != nil || len(rows) != 1 || rows[0].Tenant == nil || *rows[0].Tenant != "tenant-acme" {
				t.Fatalf("saved tenant: rows=%+v err=%v", rows, err)
			}
			loaded, err := repo.Load(ctx, "invoice-42", nil)
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Total != 1250 || loaded.CustomerID != "customer-17" || loaded.Version() != (edax.Version{Initial: 1}) || loaded.Tenant() != nil || loaded.Subject() == nil || *loaded.Subject() != "customer-17" {
				t.Fatalf("unsealed Silo load changed state or ownership: %+v tenant=%v subject=%v version=%+v", loaded, loaded.Tenant(), loaded.Subject(), loaded.Version())
			}
			wantFolds := 1
			if snapshotEvery != 0 {
				wantFolds = 0
			}
			if len(loaded.TenantsSeen) != wantFolds {
				t.Fatalf("load did not use the intended storage path: folds=%d want=%d", len(loaded.TenantsSeen), wantFolds)
			}
		})
	}
}

func TestRepositorySnapshotFallbackAndEmptySaves(t *testing.T) {
	for _, test := range []struct {
		name, snapshot string
		folds          int
	}{
		{name: "no_snapshot", folds: 2},
		{name: "snapshot_then_later_event", snapshot: `{"Total":1250,"CustomerID":"customer-17"}`, folds: 1},
		{name: "unreadable_snapshot_replays_from_zero", snapshot: `{"Total":`, folds: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := memory.New()
			defer db.Close()
			store, err := db.EventStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return new(invoiceIssued) }})
			if err != nil {
				t.Fatal(err)
			}
			repo, err := edax.NewRepository[*invoice](store, codec, invoiceCodec{}, edax.RepositoryOptions{SnapshotEvery: edax.Ptr(uint64(0))})
			if err != nil {
				t.Fatal(err)
			}
			empty, err := repo.Load(ctx, "invoice-42", nil)
			if err != nil {
				t.Fatal(err)
			}
			if empty.ID() != "invoice-42" || empty.Total != 0 || empty.Version() != (edax.Version{}) || len(empty.Pending()) != 0 {
				t.Fatalf("missing aggregate: %+v", empty)
			}
			rows, err := repo.Save(ctx, empty, edax.SaveOptions{})
			if err != nil || len(rows) != 0 {
				t.Fatalf("empty save: %v %v", rows, err)
			}
			for index, total := range []int{1250, 1500} {
				if err := edax.Apply(empty, &invoiceIssued{Total: total, CustomerID: "customer-17"}); err != nil {
					t.Fatal(err)
				}
				if _, err := repo.Save(ctx, empty, edax.SaveOptions{}); err != nil {
					t.Fatal(err)
				}
				if index == 0 && test.snapshot != "" {
					if err := store.SaveSnapshot(ctx, edax.Snapshot{AggregateID: "invoice-42", TypeName: "Invoice", Version: 1, Data: []byte(test.snapshot)}); err != nil {
						t.Fatal(err)
					}
				}
			}
			loaded, err := repo.Load(ctx, "invoice-42", nil)
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Total != 1500 || loaded.CustomerID != "customer-17" || loaded.Version() != (edax.Version{Initial: 2}) || len(loaded.Pending()) != 0 || len(loaded.TenantsSeen) != test.folds {
				t.Fatalf("replayed aggregate: %+v version=%+v", loaded, loaded.Version())
			}
			before, err := store.Unpublished(ctx, 10)
			if err != nil {
				t.Fatal(err)
			}
			rows, err = repo.Save(ctx, loaded, edax.SaveOptions{})
			if err != nil || len(rows) != 0 {
				t.Fatalf("unchanged save: %v %v", rows, err)
			}
			stale := &invoice{Aggregate: edax.NewAggregate("invoice-42")}
			if _, err := repo.Save(ctx, stale, edax.SaveOptions{}); !errors.Is(err, edax.ErrConcurrency) {
				t.Fatalf("empty stale save: %v", err)
			}
			after, err := store.Unpublished(ctx, 10)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("empty save changed outbox: %v %v", after, err)
			}
			if err := store.SaveSnapshot(ctx, edax.Snapshot{AggregateID: "invoice-42", TypeName: "Invoice", Version: 3, Data: []byte(`{"Total":9999}`)}); !errors.Is(err, edax.ErrCodec) {
				t.Fatalf("snapshot ahead of stream: %v", err)
			}
		})
	}
}

type interruptedAppend struct {
	edax.EventStore
	interrupt bool
}

func (s *interruptedAppend) Append(ctx context.Context, key string, expected uint64, drafts []edax.EventDraft) ([]edax.EventMessage, error) {
	rows, err := s.EventStore.Append(ctx, key, expected, drafts)
	if err == nil && s.interrupt {
		// Lose the staged transaction before the repository attempts its commit.
		tx, ok := edax.CurrentTransaction(ctx, s.UnitOfWork())
		if !ok {
			return nil, edax.ErrBadHandle
		}
		err = tx.Rollback(ctx)
	}
	return rows, err
}

func TestRepositoryFailedSaveKeepsPendingUntilCommit(t *testing.T) {
	for _, ambient := range []bool{false, true} {
		name := "own_commit_fails"
		if ambient {
			name = "host_rolls_back_after_save"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := memory.New()
			defer db.Close()
			store, err := db.EventStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			wrapped := &interruptedAppend{EventStore: store, interrupt: !ambient}
			codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return new(invoiceIssued) }})
			if err != nil {
				t.Fatal(err)
			}
			repo, err := edax.NewRepository[*invoice](wrapped, codec, invoiceCodec{}, edax.RepositoryOptions{SnapshotEvery: edax.Ptr(uint64(0))})
			if err != nil {
				t.Fatal(err)
			}
			aggregate := &invoice{Aggregate: edax.NewAggregate("invoice-42")}
			event := &invoiceIssued{Total: 1250, CustomerID: "customer-17"}
			if err := edax.Apply(aggregate, event); err != nil {
				t.Fatal(err)
			}
			if ambient {
				err = edax.WithinTransaction(ctx, db, func(txctx context.Context) error {
					if _, err := repo.Save(txctx, aggregate, edax.SaveOptions{}); err != nil {
						return err
					}
					return edax.ErrStore
				})
				if !errors.Is(err, edax.ErrStore) || aggregate.Version() != (edax.Version{Initial: 1}) || len(aggregate.Pending()) != 0 {
					t.Fatalf("ambient result: %v %+v", err, aggregate.Version())
				}
			} else {
				rows, saveErr := repo.Save(ctx, aggregate, edax.SaveOptions{})
				if !errors.Is(saveErr, edax.ErrBadHandle) || len(rows) != 0 || aggregate.Version() != (edax.Version{Updates: 1}) || !reflect.DeepEqual(aggregate.Pending(), []any{event}) {
					t.Fatalf("failed save: %v %+v pending=%v", saveErr, aggregate.Version(), aggregate.Pending())
				}
			}
			rows, err := store.Load(ctx, "invoice-42", 0)
			if err != nil || len(rows) != 0 {
				t.Fatalf("rolled-back events: %v %v", rows, err)
			}
			outbox, err := store.Unpublished(ctx, 10)
			if err != nil || len(outbox) != 0 {
				t.Fatalf("rolled-back outbox: %v %v", outbox, err)
			}
			wrapped.interrupt = false
			if ambient {
				// Caller-owned rollback requires discarding the aggregate before retry.
				aggregate, err = repo.Load(ctx, "invoice-42", nil)
				if err != nil {
					t.Fatal(err)
				}
				if aggregate.Version() != (edax.Version{}) || aggregate.Total != 0 {
					t.Fatalf("reload: %+v", aggregate)
				}
				if err := edax.Apply(aggregate, event); err != nil {
					t.Fatal(err)
				}
			}
			rows, err = repo.Save(ctx, aggregate, edax.SaveOptions{})
			if err != nil || len(rows) != 1 || aggregate.Version() != (edax.Version{Initial: 1}) || len(aggregate.Pending()) != 0 {
				t.Fatalf("retry: %v %v %+v", rows, err, aggregate.Version())
			}
			loaded, err := repo.Load(ctx, "invoice-42", nil)
			if err != nil || loaded.Total != event.Total || loaded.CustomerID != event.CustomerID {
				t.Fatalf("persisted retry: %+v %v", loaded, err)
			}
		})
	}
}

type invoice struct {
	edax.Aggregate
	Total      int
	CustomerID string
	// TenantsSeen records the tenant each Transition saw, or "" for none.
	TenantsSeen []string `json:"-"`
}

func (aggregate *invoice) Transition(event any) error {
	issued, ok := event.(*invoiceIssued)
	if !ok {
		return edax.ErrCodec
	}
	seen := ""
	if tenant := aggregate.Tenant(); tenant != nil {
		seen = *tenant
	}
	aggregate.TenantsSeen = append(aggregate.TenantsSeen, seen)
	aggregate.Total = issued.Total
	aggregate.CustomerID = issued.CustomerID
	return nil
}

type invoiceCodec struct{}

func (invoiceCodec) TypeName() string { return "Invoice" }
func (invoiceCodec) Load(id string, version uint64, data []byte) (*invoice, error) {
	aggregate := &invoice{Aggregate: edax.NewAggregate(id)}
	if data != nil {
		if err := json.Unmarshal(data, aggregate); err != nil {
			return nil, err
		}
	}
	return aggregate, nil
}
func (invoiceCodec) Dump(aggregate *invoice) ([]byte, error) { return json.Marshal(aggregate) }

func TestRepositorySnapshotReplayAndConflict(t *testing.T) {
	database := memory.New()
	defer database.Close()
	store, _ := database.EventStore()
	defer store.Close()
	commands, _ := database.CommandOutbox()
	defer commands.Close()
	codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return &invoiceIssued{} }})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := edax.NewRepository[*invoice](store, codec, invoiceCodec{}, edax.RepositoryOptions{Tenancy: "pool", Commands: commands})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tenant := edax.Ptr("tenant-acme")
	aggregate, err := repository.Load(ctx, "invoice-42", tenant)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := repository.Load(ctx, "invoice-42", tenant)
	if err != nil {
		t.Fatal(err)
	}
	if err = edax.Apply(aggregate, &invoiceIssued{Total: 1250, CustomerID: "customer-17"}); err != nil {
		t.Fatal(err)
	}
	command, err := edax.NewCommandMessage(ctx, edax.Payload{Name: "SendInvoice", Version: 1, Data: []byte(`{"invoice_id":"invoice-42"}`)}, edax.CommandOptions{})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := repository.Save(ctx, aggregate, edax.SaveOptions{Commands: []edax.CommandMessage{command}})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || *messages[0].Sequence != 1 || *messages[0].Tenant != *tenant || len(aggregate.Pending()) != 0 || aggregate.Version().Initial != 1 {
		t.Fatalf("saved %+v state=%+v", messages, aggregate.Version())
	}
	if held, heldErr := commands.Unpublished(ctx, 10); heldErr != nil || len(held) != 0 {
		t.Fatalf("a command waits behind its save's event: %+v %v", held, heldErr)
	}
	if err = store.MarkPublished(ctx, []string{messages[0].ID}); err != nil {
		t.Fatal(err)
	}
	outbox, err := commands.Unpublished(ctx, 10)
	if err != nil || len(outbox) != 1 || *outbox[0].Tenant != *tenant {
		t.Fatalf("outbox %+v %v", outbox, err)
	}
	loaded, err := repository.Load(ctx, "invoice-42", tenant)
	if err != nil || loaded.Total != 1250 || loaded.Version().Initial != 1 {
		t.Fatalf("load %+v %v", loaded, err)
	}
	key, _ := kernel.TenantKey("pool", "invoice-42", tenant)
	if err = store.SaveSnapshot(ctx, edax.Snapshot{AggregateID: key, TypeName: "OldInvoice", Version: 1, Data: []byte("unreadable")}); err != nil {
		t.Fatal(err)
	}
	loaded, err = repository.Load(ctx, "invoice-42", tenant)
	if err != nil || loaded.Total != 1250 {
		t.Fatalf("snapshot fallback %+v %v", loaded, err)
	}
	if err = edax.Apply(stale, &invoiceIssued{Total: 500, CustomerID: "customer-17"}); err != nil {
		t.Fatal(err)
	}
	if _, err = repository.Save(ctx, stale, edax.SaveOptions{}); !errors.Is(err, edax.ErrConcurrency) {
		t.Fatalf("stale save: %v", err)
	}
	if len(stale.Pending()) != 1 {
		t.Fatal("failed save erased pending event")
	}
	other, err := repository.Load(ctx, "invoice-42", edax.Ptr("tenant-other"))
	if err != nil || other.Total != 0 {
		t.Fatalf("tenant isolation %+v %v", other, err)
	}
	if _, err = repository.Load(ctx, "invoice-42", nil); !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("missing tenant: %v", err)
	}
}

func TestRepositoryReplayedTransitionSeesTheLoadedTenant(t *testing.T) {
	// No snapshot, so the load folds both events: each must see the tenant a live Apply saw.
	database := memory.New()
	defer database.Close()
	store, _ := database.EventStore()
	defer store.Close()
	codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return &invoiceIssued{} }})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := edax.NewRepository[*invoice](store, codec, invoiceCodec{}, edax.RepositoryOptions{Tenancy: "pool", SnapshotEvery: edax.Ptr(uint64(0))})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tenant := edax.Ptr("tenant-acme")
	live, err := repository.Load(ctx, "invoice-42", tenant)
	if err != nil {
		t.Fatal(err)
	}
	if err = edax.Apply(live, &invoiceIssued{Total: 1250}, &invoiceIssued{Total: 1500}); err != nil {
		t.Fatal(err)
	}
	if _, err = repository.Save(ctx, live, edax.SaveOptions{}); err != nil {
		t.Fatal(err)
	}
	replayed, err := repository.Load(ctx, "invoice-42", tenant)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{*tenant, *tenant}
	if !slices.Equal(live.TenantsSeen, want) || !slices.Equal(replayed.TenantsSeen, want) {
		t.Fatalf("live saw %q, replay saw %q, want %q", live.TenantsSeen, replayed.TenantsSeen, want)
	}
}

type loadFailureStore struct {
	edax.EventStore
	owner    *loadFailureUnit
	messages []edax.EventMessage
	loadErr  error
}

func (s *loadFailureStore) UnitOfWork() edax.UnitOfWork { return s.owner }
func (s *loadFailureStore) LoadSnapshot(context.Context, string) (*edax.Snapshot, error) {
	return nil, nil
}
func (s *loadFailureStore) Load(context.Context, string, uint64) ([]edax.EventMessage, error) {
	return s.messages, s.loadErr
}

type loadFailureUnit struct {
	commitErr          error
	commits, rollbacks int
}

func (u *loadFailureUnit) Begin(context.Context) (edax.Tx, error) { return u, nil }
func (u *loadFailureUnit) Commit(context.Context) error           { u.commits++; return u.commitErr }
func (u *loadFailureUnit) Rollback(context.Context) error         { u.rollbacks++; return nil }

type trackingInvoiceCodec struct {
	invoiceCodec
	created *invoice
}

func (c *trackingInvoiceCodec) Load(id string, version uint64, data []byte) (*invoice, error) {
	value, err := c.invoiceCodec.Load(id, version, data)
	c.created = value
	return value, err
}

func TestRepositoryLoadDiscardsAggregateOnFailure(t *testing.T) {
	for _, test := range []struct {
		name               string
		loadErr, commitErr error
		secondData         string
		secondName         string
		want               error
		partial            bool
	}{
		{name: "stream_read", loadErr: edax.ErrStore, want: edax.ErrStore},
		{name: "payload_decode", secondName: "InvoiceIssued", secondData: `{"total":`, want: edax.ErrCodec, partial: true},
		{name: "transition", secondName: "AccountOpened", secondData: `{"owner":"Ada","currency":"EUR"}`, want: edax.ErrCodec, partial: true},
		{name: "commit", secondName: "InvoiceIssued", secondData: `{"total":1500,"customer_id":"customer-17"}`, commitErr: edax.ErrStore, want: edax.ErrStore, partial: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return new(invoiceIssued) }, "AccountOpened": func() any { return new(accountOpened) }})
			if err != nil {
				t.Fatal(err)
			}
			unit := &loadFailureUnit{commitErr: test.commitErr}
			store := &loadFailureStore{owner: unit, loadErr: test.loadErr, messages: []edax.EventMessage{
				{ID: "invoice-issued-42", AggregateID: "invoice-42", Sequence: edax.Ptr(uint64(1)), Name: "InvoiceIssued", Version: 1, Data: []byte(`{"total":1250,"customer_id":"customer-17"}`)},
				{ID: "invoice-updated-42", AggregateID: "invoice-42", Sequence: edax.Ptr(uint64(2)), Name: test.secondName, Version: 1, Data: []byte(test.secondData)},
			}}
			aggregates := &trackingInvoiceCodec{}
			repo, err := edax.NewRepository[*invoice](store, codec, aggregates, edax.RepositoryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := repo.Load(context.Background(), "invoice-42", nil)
			if got != nil || !errors.Is(err, test.want) {
				t.Fatalf("partial result %+v: %v", got, err)
			}
			if test.partial && (aggregates.created == nil || aggregates.created.Total == 0) {
				t.Fatal("fixture did not mutate aggregate before failure")
			}
			if unit.rollbacks != 1 {
				t.Fatalf("rollbacks=%d", unit.rollbacks)
			}
			if test.commitErr != nil && unit.commits != 1 {
				t.Fatal("fixture did not reach commit")
			}
		})
	}
}

// countingStore counts the reads a repository makes.
type countingStore struct {
	edax.EventStore
	reads int
}

func (s *countingStore) Load(ctx context.Context, id string, after uint64) ([]edax.EventMessage, error) {
	s.reads++
	return s.EventStore.Load(ctx, id, after)
}

func (s *countingStore) LoadSnapshot(ctx context.Context, id string) (*edax.Snapshot, error) {
	s.reads++
	return s.EventStore.LoadSnapshot(ctx, id)
}

func TestRepositoryCreateStartsAnAggregateWithoutReading(t *testing.T) {
	for _, test := range []struct {
		name    string
		tenancy edax.Tenancy
		tenant  *string
		fails   bool
	}{
		{name: "a pool refuses a create with no tenant", tenancy: "pool", fails: true},
		{name: "a bridge refuses one too", tenancy: "bridge", fails: true},
		{name: "a silo still checks a tenant it is given", tenancy: "silo", tenant: edax.Ptr(""), fails: true},
		{name: "a silo expects none", tenancy: "silo"},
		{name: "a pool with a tenant is fine", tenancy: "pool", tenant: edax.Ptr("acme")},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			database := memory.New()
			defer database.Close()
			inner, err := database.EventStore()
			if err != nil {
				t.Fatal(err)
			}
			defer inner.Close()
			store := &countingStore{EventStore: inner}
			codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return new(invoiceIssued) }})
			if err != nil {
				t.Fatal(err)
			}
			repo, err := edax.NewRepository[*invoice](store, codec, invoiceCodec{}, edax.RepositoryOptions{Tenancy: test.tenancy})
			if err != nil {
				t.Fatal(err)
			}
			created, err := repo.Create("invoice-42", test.tenant)
			if test.fails {
				if !errors.Is(err, edax.ErrCodec) {
					t.Fatalf("create: %v", err)
				}
				return
			}
			if err != nil || created.Version() != (edax.Version{}) || !reflect.DeepEqual(created.Tenant(), test.tenant) || store.reads != 0 {
				t.Fatalf("create %+v %v reads=%d", created, err, store.reads)
			}
			if err = edax.Apply(created, &invoiceIssued{Total: 10}); err != nil {
				t.Fatal(err)
			}
			stored, err := repo.Save(ctx, created, edax.SaveOptions{})
			if err != nil || len(stored) != 1 || !reflect.DeepEqual(stored[0].Tenant, test.tenant) {
				t.Fatalf("save stamps the tenant: %+v %v", stored, err)
			}
			again, _ := repo.Create("invoice-42", test.tenant)
			if err = edax.Apply(again, &invoiceIssued{Total: 5}); err != nil {
				t.Fatal(err)
			}
			if _, err = repo.Save(ctx, again, edax.SaveOptions{}); !errors.Is(err, edax.ErrConcurrency) {
				t.Fatalf("existing id: %v", err)
			}
		})
	}
}

// eventStoreOnly hides a store's LockStream, so it cannot back CommandFenceLock.
type eventStoreOnly struct{ edax.EventStore }

// fenceRepository builds a repository over a fresh memory database under fence, with events as the
// event store when given, else the database's own.
func fenceRepository(t *testing.T, fence edax.CommandFence, events func(edax.EventStore) edax.EventStore) (*edax.Repository[*invoice], edax.CommandOutbox, error) {
	t.Helper()
	database := memory.New()
	t.Cleanup(func() { database.Close() })
	store, _ := database.EventStore()
	t.Cleanup(func() { store.Close() })
	commands, _ := database.CommandOutbox()
	t.Cleanup(func() { commands.Close() })
	codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return &invoiceIssued{} }})
	if err != nil {
		t.Fatal(err)
	}
	var backing edax.EventStore = store
	if events != nil {
		backing = events(store)
	}
	repository, err := edax.NewRepository[*invoice](backing, codec, invoiceCodec{}, edax.RepositoryOptions{Commands: commands, CommandFence: fence})
	return repository, commands, err
}

// sendInvoice saves a commands-only change of invoice-42 and returns what the outbox then holds.
func sendInvoice(t *testing.T, repository *edax.Repository[*invoice], commands edax.CommandOutbox) ([]edax.CommandMessage, edax.CommandMessage, error) {
	t.Helper()
	ctx := context.Background()
	aggregate, err := repository.Load(ctx, "invoice-42", nil)
	if err != nil {
		t.Fatal(err)
	}
	command, err := edax.NewCommandMessage(ctx, edax.Payload{Name: "SendInvoice", Version: 1, Data: []byte(`{"invoice_id":"invoice-42"}`)}, edax.CommandOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, saveErr := repository.Save(ctx, aggregate, edax.SaveOptions{Commands: []edax.CommandMessage{command}})
	queued, err := commands.Unpublished(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	return queued, command, saveErr
}

func TestRepositoryCommandFenceWiringRefusals(t *testing.T) {
	hideLock := func(store edax.EventStore) edax.EventStore { return eventStoreOnly{store} }
	for _, c := range []struct {
		name   string
		fence  edax.CommandFence
		events func(edax.EventStore) edax.EventStore
		want   string
	}{
		{name: "unknown_fence", fence: "none", want: `unknown command fence "none"`},
		{name: "lock_fence_without_a_stream_locker", fence: edax.CommandFenceLock, events: hideLock, want: "implements StreamLocker"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := fenceRepository(t, c.fence, c.events)
			var refused *edax.Error
			if !errors.As(err, &refused) || refused.Code != edax.CodeBadHandle || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("build: %v", err)
			}
		})
	}
}

func TestEventFenceRefusesACommandsOnlySave(t *testing.T) {
	repository, commands, err := fenceRepository(t, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	queued, _, err := sendInvoice(t, repository, commands)
	if err == nil || !strings.Contains(err.Error(), "must append an event") || len(queued) != 0 {
		t.Fatalf("save: %v, queued %d", err, len(queued))
	}
}

func TestLockFenceTakesACommandsOnlySave(t *testing.T) {
	repository, commands, err := fenceRepository(t, edax.CommandFenceLock, nil)
	if err != nil {
		t.Fatal(err)
	}
	queued, command, err := sendInvoice(t, repository, commands)
	if err != nil || len(queued) != 1 || queued[0].ID != command.ID {
		t.Fatalf("save: %v, queued %+v", err, queued)
	}
}
