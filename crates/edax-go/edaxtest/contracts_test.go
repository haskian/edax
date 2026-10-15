package edaxtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/backend/redis"
	"github.com/haskian/edax/crates/edax-go/edaxtest"
	goredis "github.com/redis/go-redis/v9"
)

func TestMemoryContracts(t *testing.T) {
	t.Run("events", func(t *testing.T) {
		edaxtest.EventStore(t, func(t testing.TB) edaxtest.EventBackend { return edaxtest.MemoryFixture(t).Events })
	})
	t.Run("ledger", func(t *testing.T) {
		edaxtest.IdempotencyStore(t, func(t testing.TB) edax.IdempotencyStore { return edaxtest.MemoryFixture(t).Ledger })
	})
	t.Run("projection", func(t *testing.T) {
		edaxtest.ProjectionStore(t, func(t testing.TB) edax.ProjectionStore { return edaxtest.MemoryFixture(t).Projections })
		edaxtest.NamedProjectionStores(t, edaxtest.MemoryNamedProjections)
	})
	t.Run("command_outbox", func(t *testing.T) {
		edaxtest.CommandOutbox(t, func(t testing.TB) edax.CommandOutbox { return edaxtest.MemoryFixture(t).Commands })
	})
	t.Run("saga", func(t *testing.T) {
		edaxtest.SagaStore(t, func(t testing.TB) edax.SagaStore { return edaxtest.MemoryFixture(t).Sagas })
	})
	t.Run("atomicity", func(t *testing.T) { edaxtest.Transactions(t, edaxtest.MemoryFixture) })
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

func TestRedisLedgerContract(t *testing.T) {
	edaxtest.IdempotencyStore(t, func(t testing.TB) edax.IdempotencyStore {
		server := miniredis.RunT(t)
		client := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
		t.Cleanup(func() { _ = client.Close() })
		store, err := redis.New(redis.NewClient(client), redis.Config{Prefix: "billing:contract:idem:", TTL: time.Hour, ClaimTTL: time.Minute, OpTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}
