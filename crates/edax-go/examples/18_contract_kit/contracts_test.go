// Run with: go test -race -v ./examples/18_contract_kit
package contracts_test

import (
	"testing"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/edaxtest"
)

// Replace this factory with your database adapter. Each invocation needs an empty,
// isolated backend. Cleanup runs in reverse order: store first, database second.
func TestProjectionStore(t *testing.T) {
	edaxtest.ProjectionStore(t, func(t testing.TB) edax.ProjectionStore {
		db := memory.New()
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		store, err := db.ProjectionStore("read_model")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		return store
	})
	t.Log("ok: projection store passed the reusable contract")
}

func TestEventStore(t *testing.T) {
	edaxtest.EventStore(t, func(t testing.TB) edaxtest.EventBackend {
		db := memory.New()
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		store, err := db.EventStore()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		return store
	})
	t.Log("ok: event store passed the reusable contract")
}
