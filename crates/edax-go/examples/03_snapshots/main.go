// Loading uses the snapshot and replays its tail.
// Run from crates/edax-go: mise exec -- go run ./examples/03_snapshots
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

type deposited struct {
	Amount int `json:"amount"`
}
type account struct {
	edax.Aggregate
	Balance         int
	SnapshotVersion uint64
	Transitions     int
}

func (a *account) Transition(event any) error {
	switch event := event.(type) {
	case *deposited:
		a.Balance += event.Amount
		a.Transitions++
	default:
		return fmt.Errorf("unexpected account event %T", event)
	}
	return nil
}

type accountCodec struct{}

func (accountCodec) TypeName() string { return "Account" }
func (accountCodec) Load(id string, version uint64, data []byte) (*account, error) {
	a := &account{Aggregate: edax.NewAggregate(id)}
	if data != nil {
		value, err := strconv.Atoi(string(data))
		if err != nil {
			return nil, err
		}
		a.Balance = value
		a.SnapshotVersion = version
	}
	return a, nil
}
func (accountCodec) Dump(a *account) ([]byte, error) { return []byte(strconv.Itoa(a.Balance)), nil }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: Loading uses the snapshot and replays its tail.")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	store, err := db.EventStore()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	events, err := edax.NewJSONCodec(map[string]func() any{"Deposited": func() any { return new(deposited) }})
	if err != nil {
		return err
	}
	repo, err := edax.NewRepository(store, events, accountCodec{}, edax.RepositoryOptions{SnapshotEvery: edax.Ptr(uint64(3))})
	if err != nil {
		return err
	}

	for version := uint64(1); version <= 7; version++ {
		a, err := repo.Load(ctx, "acct-1", nil)
		if err != nil {
			return err
		}
		if err := edax.Apply(a, &deposited{10}); err != nil {
			return err
		}
		if _, err := repo.Save(ctx, a, edax.SaveOptions{}); err != nil {
			return err
		}
		snapshot, err := store.LoadSnapshot(ctx, "acct-1")
		if err != nil {
			return err
		}
		want := version / 3 * 3
		if want == 0 {
			if snapshot != nil {
				return fmt.Errorf("unexpected early snapshot at version %d", version)
			}
		} else if snapshot == nil || snapshot.Version != want {
			return fmt.Errorf("snapshot after version %d: want version %d, got %+v", version, want, snapshot)
		}
	}
	snapshot, err := store.LoadSnapshot(ctx, "acct-1")
	if err != nil {
		return err
	}
	if snapshot == nil || snapshot.Version != 6 {
		return fmt.Errorf("expected snapshot at version 6")
	}
	tail, err := store.Load(ctx, "acct-1", snapshot.Version)
	if err != nil {
		return err
	}
	loaded, err := repo.Load(ctx, "acct-1", nil)
	if err != nil {
		return err
	}
	if len(tail) != 1 || loaded.Balance != 70 || loaded.Version().Current() != 7 {
		return fmt.Errorf("snapshot replay: tail=%d balance=%d version=%d", len(tail), loaded.Balance, loaded.Version().Current())
	}
	if loaded.SnapshotVersion != 6 || loaded.Transitions != 1 {
		return fmt.Errorf("load started at snapshot %d and replayed %d events", loaded.SnapshotVersion, loaded.Transitions)
	}
	fmt.Printf("snapshot version %d + %d event -> balance %d\n", loaded.SnapshotVersion, loaded.Transitions, loaded.Balance)

	return nil
}
