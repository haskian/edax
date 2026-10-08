package edaxtest

import (
	"testing"

	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

// MemoryFixture opens every store of one fresh memory database. It is exported to this package's
// external tests too, so the conformance run and the negative controls share one backend.
func MemoryFixture(t testing.TB) TransactionFixture {
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
	return TransactionFixture{Owner: db, Events: events, Ledger: ledger, Commands: commands, Projections: projections, Sagas: sagas}
}
