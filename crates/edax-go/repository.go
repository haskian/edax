package edax

import (
	"context"
	"time"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// PayloadCodec maps domain values to named, versioned bytes.
type PayloadCodec interface {
	Marshal(value any) (Payload, error)
	Unmarshal(payload Payload) (any, error)
}

// AggregateCodec constructs aggregates and encodes their snapshot cache.
// Load receives nil data when no usable snapshot exists.
type AggregateCodec[A AggregateRoot] interface {
	TypeName() string
	Load(id string, version uint64, data []byte) (A, error)
	Dump(aggregate A) ([]byte, error)
}

// RepositoryOptions controls tenancy, snapshots, identifiers and a transactional command outbox.
// Nil SnapshotEvery snapshots each save; a pointer to zero disables snapshots.
type RepositoryOptions struct {
	Tenancy       Tenancy
	SnapshotEvery *uint64
	NewID         func() string
	Commands      CommandOutbox
}

// Repository saves events, snapshots and commands in one database transaction.
type Repository[A AggregateRoot] struct {
	store      EventStore
	events     PayloadCodec
	aggregates AggregateCodec[A]
	options    RepositoryOptions
}

// NewRepository validates codecs and requires event and command outboxes to share a database.
// The repository borrows its stores; the caller retains responsibility for closing them.
func NewRepository[A AggregateRoot](store EventStore, events PayloadCodec, aggregates AggregateCodec[A], options RepositoryOptions) (*Repository[A], error) {
	if store == nil || events == nil || aggregates == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "repository requires a store and codecs"}
	}
	if options.Tenancy == "" {
		options.Tenancy = TenancySilo
	}
	if _, err := kernel.GetTenancyRules(string(options.Tenancy)); err != nil {
		return nil, err
	}
	if options.NewID == nil {
		options.NewID = Guid
	}
	if options.SnapshotEvery == nil {
		options.SnapshotEvery = Ptr(uint64(1))
	}
	if options.Commands != nil && options.Commands.UnitOfWork() != store.UnitOfWork() {
		return nil, &Error{Code: CodeBadHandle, Message: "event and command outboxes must share a database"}
	}
	return &Repository[A]{store: store, events: events, aggregates: aggregates, options: options}, nil
}

func (r *Repository[A]) enlist(ctx context.Context, run func(context.Context) error) error {
	if _, exists := CurrentTransaction(ctx, r.store.UnitOfWork()); exists {
		return run(ctx)
	}
	return WithinTransaction(ctx, r.store.UnitOfWork(), run)
}

// Load uses a snapshot when its codec can read it, then checks and replays the dense event stream.
// Any failure, including transaction commit failure, returns the zero aggregate value.
func (r *Repository[A]) Load(ctx context.Context, id string, tenant *string) (aggregate A, err error) {
	started := time.Now()
	defer func() {
		ObserveContext(ctx, CurrentObserver(ctx), Observation{Operation: "repository.load", Name: r.aggregates.TypeName(), ID: id, Duration: time.Since(started), Err: err})
	}()
	key, err := kernel.TenantKey(string(r.options.Tenancy), id, tenant)
	if err != nil {
		return aggregate, err
	}
	err = r.enlist(ctx, func(txctx context.Context) error {
		snapshot, loadErr := r.store.LoadSnapshot(txctx, key)
		if loadErr != nil {
			return loadErr
		}
		base := uint64(0)
		cached := false
		if snapshot != nil && snapshot.TypeName == r.aggregates.TypeName() {
			aggregate, loadErr = r.aggregates.Load(id, snapshot.Version, snapshot.Data)
			if loadErr == nil {
				base = snapshot.Version
				cached = true
			}
		}
		if !cached {
			aggregate, loadErr = r.aggregates.Load(id, 0, nil)
			if loadErr != nil {
				return loadErr
			}
		}
		messages, loadErr := r.store.Load(txctx, key, base)
		if loadErr != nil {
			return loadErr
		}
		sequences := make([]*uint64, len(messages))
		for index, message := range messages {
			sequences[index] = message.Sequence
		}
		version, loadErr := kernel.CheckSequenceBatch(base, sequences)
		if loadErr != nil {
			return loadErr
		}
		// Before the fold, so a replayed Transition sees the tenant a live Apply sees.
		standAt(aggregate, base, tenant)
		for _, message := range messages {
			event, decodeErr := r.events.Unmarshal(Payload{Name: message.Name, Version: message.Version, Data: message.Data})
			if decodeErr != nil {
				return decodeErr
			}
			if applyErr := aggregate.Transition(event); applyErr != nil {
				return applyErr
			}
		}
		standAt(aggregate, version, tenant)
		return nil
	})
	if err != nil {
		var zero A
		return zero, err
	}
	return aggregate, nil
}

// SaveOptions supplies lineage, metadata and commands to save with an aggregate batch.
type SaveOptions struct {
	Lineage    *Lineage
	Extensions map[string]string
	Metadata   map[string]string
	OccurredAt *int64
	Commands   []CommandMessage
}

// Save clears pending changes after its writes succeed. When ctx already holds
// a transaction, the caller owns commit and must discard the aggregate on rollback.
func (r *Repository[A]) Save(ctx context.Context, aggregate A, options SaveOptions) (stored []EventMessage, err error) {
	state := aggregate.AggregateState()
	started := time.Now()
	defer func() {
		ObserveContext(ctx, CurrentObserver(ctx), Observation{Operation: "repository.save", Name: r.aggregates.TypeName(), ID: state.id, Duration: time.Since(started), Err: err})
	}()
	key, err := kernel.TenantKey(string(r.options.Tenancy), state.id, state.tenant)
	if err != nil {
		return nil, err
	}
	if len(options.Commands) > 0 && r.options.Commands == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "saving commands requires a command outbox"}
	}
	payloads := make([]Payload, len(state.pending))
	ids := make([]string, len(state.pending))
	for index, event := range state.pending {
		payloads[index], err = r.events.Marshal(event)
		if err != nil {
			return nil, err
		}
		ids[index] = r.options.NewID()
	}
	flow, err := messageLineage(ctx, options.Lineage)
	if err != nil {
		return nil, err
	}
	batch, err := kernel.GetDraftBatch(state.id, state.initial, *r.options.SnapshotEvery, payloads, ids, flow, options.Extensions, options.Metadata, state.tenant, options.OccurredAt)
	if err != nil {
		return nil, err
	}
	commands := make([]CommandMessage, len(options.Commands))
	for index, command := range options.Commands {
		owned, ownErr := kernel.OwnCommand(command, state.tenant)
		if ownErr != nil {
			return nil, ownErr
		}
		commands[index] = *owned
	}
	var snapshot *Snapshot
	if batch.Snapshot {
		data, dumpErr := r.aggregates.Dump(aggregate)
		if dumpErr != nil {
			return nil, dumpErr
		}
		snapshot = &Snapshot{AggregateID: key, TypeName: r.aggregates.TypeName(), Version: batch.Version, Data: data}
	}
	err = r.enlist(ctx, func(txctx context.Context) error {
		var saveErr error
		stored, saveErr = r.store.Append(txctx, key, state.initial, batch.Drafts)
		if saveErr != nil {
			return saveErr
		}
		if len(commands) > 0 {
			if saveErr = r.options.Commands.Enqueue(txctx, commands); saveErr != nil {
				return saveErr
			}
		}
		if snapshot != nil {
			return r.store.SaveSnapshot(txctx, *snapshot)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	standAt(aggregate, batch.Version, state.tenant)
	return stored, nil
}
