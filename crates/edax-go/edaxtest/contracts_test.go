package edaxtest_test

import (
	"context"
	"testing"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/edaxtest"
)

func fixture(t testing.TB) edaxtest.TransactionFixture {
	t.Helper()
	db := memory.New()
	t.Cleanup(func() { _ = db.Close() })
	events, err := db.EventStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = events.Close() })
	ledger, err := db.IdempotencyStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ledger.Close() })
	commands, err := db.CommandOutbox()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commands.Close() })
	projections, err := db.ProjectionStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = projections.Close() })
	sagas, err := db.SagaStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sagas.Close() })
	return edaxtest.TransactionFixture{Owner: db, Events: events, Ledger: ledger, Commands: commands, Projections: projections, Sagas: sagas}
}
func TestMemoryContracts(t *testing.T) {
	t.Run("events", func(t *testing.T) {
		edaxtest.EventStore(t, func(t testing.TB) edaxtest.EventBackend { return fixture(t).Events })
	})
	t.Run("ledger", func(t *testing.T) {
		edaxtest.IdempotencyStore(t, func(t testing.TB) edax.IdempotencyStore { return fixture(t).Ledger })
	})
	t.Run("projection", func(t *testing.T) {
		edaxtest.ProjectionStore(t, func(t testing.TB) edax.ProjectionStore { return fixture(t).Projections })
	})
	t.Run("command_outbox", func(t *testing.T) {
		edaxtest.CommandOutbox(t, func(t testing.TB) edax.CommandOutbox { return fixture(t).Commands })
	})
	t.Run("saga", func(t *testing.T) {
		edaxtest.SagaStore(t, func(t testing.TB) edax.SagaStore { return fixture(t).Sagas })
	})
	t.Run("atomicity", func(t *testing.T) { edaxtest.Transactions(t, fixture) })
	t.Run("broker", func(t *testing.T) {
		edaxtest.EventBroker(t, func(t testing.TB) edaxtest.EventBrokerFixture {
			broker, err := memory.NewBroker(1, "silo")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = broker.Close() })
			return edaxtest.EventBrokerFixture{Publish: broker.PublishEvent, Poll: func(ctx context.Context) (*edax.Delivery[edax.EventMessage], error) {
				event, _, err := broker.Poll(ctx, "contract")
				return event, err
			}}
		})
	})
}
