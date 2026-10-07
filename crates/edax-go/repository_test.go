package edax_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

type invoiceIssued struct {
	Total      int    `json:"total"`
	CustomerID string `json:"customer_id"`
}
type invoice struct {
	edax.Aggregate
	Total      int
	CustomerID string
}

func (aggregate *invoice) Transition(event any) error {
	issued, ok := event.(*invoiceIssued)
	if !ok {
		return edax.ErrCodec
	}
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
