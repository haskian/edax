package edax

import (
	"bytes"
	"context"
	"encoding/json"
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
	Sealing       *Sealing
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
	} else {
		options.SnapshotEvery = Ptr(*options.SnapshotEvery)
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

// Create returns a new aggregate, empty at version 0 and carrying tenant. It persists and reads nothing,
// so it takes no context and reports no observation; Save writes it. The tenant is checked as Load checks it.
// Use it for an id this handler run minted. An id from outside may already exist: its Save fails with the
// transient ErrConcurrency, so a retry re-runs the handler into the same conflict until its budget runs out.
// Load such an id and check Version().Initial == 0 instead.
func (r *Repository[A]) Create(id string, tenant *string) (A, error) {
	var zero A
	if _, err := kernel.TenantKey(string(r.options.Tenancy), id, tenant); err != nil {
		return zero, err
	}
	aggregate, err := r.aggregates.Load(id, 0, nil)
	if err != nil {
		return zero, err
	}
	standAt(aggregate, 0, tenant)
	return aggregate, nil
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
		var subject *string
		ownerKnown := false
		if snapshot != nil && snapshot.TypeName == r.aggregates.TypeName() {
			data := snapshot.Data
			if r.options.Sealing != nil {
				if snapshot.Encryption != nil || snapshot.Tenant != nil {
					if loadErr = kernel.CheckSealedOwner(true, tenant, snapshot.Tenant); loadErr != nil {
						return loadErr
					}
				}
				data, loadErr = r.options.Sealing.OpenState(txctx, StateContext{Kind: "snapshot", Identity: key, TypeName: snapshot.TypeName, Version: snapshot.Version, Tenant: tenant, Subject: snapshot.Subject}, snapshot.Encryption, snapshot.Data)
				if loadErr != nil {
					return loadErr
				}
			} else if snapshot.Encryption != nil {
				return &Error{Code: CodeBadHandle, Message: "encrypted snapshots require repository sealing"}
			}
			subject, ownerKnown = snapshot.Subject, true
			aggregate, loadErr = r.aggregates.Load(id, snapshot.Version, data)
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
		// Check the complete batch before any application transition can observe mixed ownership.
		for _, message := range messages {
			if r.options.Sealing != nil {
				if loadErr = kernel.CheckSealedOwner(true, tenant, message.Tenant); loadErr != nil {
					return loadErr
				}
			}
			if loadErr = kernel.CheckSealedOwner(ownerKnown, subject, message.Subject); loadErr != nil {
				return loadErr
			}
			if !ownerKnown {
				subject, ownerKnown = message.Subject, true
			}
		}
		// Before the fold, so a replayed Transition sees the tenant a live Apply sees.
		standAt(aggregate, base, tenant)
		state := aggregate.AggregateState()
		state.subject = nil
		if subject != nil {
			state.subject = Ptr(*subject)
		}
		state.ownerCommitted = ownerKnown
		for _, message := range messages {
			data := message.Data
			if r.options.Sealing != nil {
				data, loadErr = r.options.Sealing.OpenEvent(txctx, message)
				if loadErr != nil {
					return loadErr
				}
			} else if message.Encryption != nil {
				return &Error{Code: CodeBadHandle, Message: "encrypted events require repository sealing"}
			}
			event, decodeErr := r.events.Unmarshal(Payload{Name: message.Name, Version: message.Version, Data: data})
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

// LoadOwned supplies the initial owner for an empty stream and verifies an existing stream's owner.
func (r *Repository[A]) LoadOwned(ctx context.Context, id string, tenant, subject *string) (A, error) {
	aggregate, err := r.Load(ctx, id, tenant)
	if err == nil {
		err = aggregate.AggregateState().SetSubject(subject)
	}
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

// A prepared write owns its bytes until the transaction's outcome is known. It is process-local;
// after a restart the application reloads persisted state before rebuilding pending domain events.
type preparedAggregateSave struct {
	owner     UnitOfWork
	sealing   *Sealing
	signature []byte
	drafts    []EventDraft
	commands  []CommandMessage
	snapshot  *Snapshot
	version   uint64
	attempted bool
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
	if r.options.Sealing != nil {
		return r.saveProtected(ctx, aggregate, key, options)
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
	batch, err := kernel.GetDraftBatch(state.id, state.initial, *r.options.SnapshotEvery, payloads, ids, flow, options.Extensions, options.Metadata, state.tenant, options.OccurredAt, state.subject)
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
		snapshot = &Snapshot{AggregateID: key, TypeName: r.aggregates.TypeName(), Version: batch.Version, Data: data, Tenant: state.Tenant(), Subject: state.Subject()}
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

func (r *Repository[A]) saveProtected(ctx context.Context, aggregate A, stream string, options SaveOptions) ([]EventMessage, error) {
	state := aggregate.AggregateState()
	scope, err := r.options.Sealing.Scope(state.tenant, state.subject)
	if err != nil {
		return nil, err
	}
	ctx, release, err := r.options.Sealing.Guard(ctx, scope)
	if err != nil {
		return nil, err
	}
	if !HoldTransactionResource(ctx, r.store.UnitOfWork(), release) {
		defer release()
	}

	payloads := make([]Payload, len(state.pending))
	for index, event := range state.pending {
		payloads[index], err = r.events.Marshal(event)
		if err != nil {
			return nil, err
		}
	}
	prepared := state.prepared
	var snapshotPlaintext []byte
	if prepared == nil {
		prepared, snapshotPlaintext, err = r.prepareProtected(ctx, aggregate, stream, payloads, options)
	} else if prepared.snapshot != nil {
		snapshotPlaintext, err = r.aggregates.Dump(aggregate)
	}
	if err != nil {
		return nil, err
	}
	signature, err := json.Marshal(struct {
		Payloads                            []Payload
		Options                             SaveOptions
		Snapshot                            []byte
		Tenant, Subject                     *string
		Initial                             uint64
		TypeName, Stream, Namespace, Cipher string
	}{payloads, options, snapshotPlaintext, state.tenant, state.subject, state.initial, r.aggregates.TypeName(), stream, r.options.Sealing.KeyNamespace, r.options.Sealing.Cipher})
	if err != nil {
		return nil, &Error{Code: CodeCodec, Message: "cannot capture prepared save inputs"}
	}
	if state.prepared != nil {
		if prepared.owner != r.store.UnitOfWork() || prepared.sealing != r.options.Sealing || !bytes.Equal(prepared.signature, signature) {
			return nil, &Error{Code: CodeBadHandle, Message: "prepared save inputs changed; discard and reload the aggregate"}
		}
		if prepared.attempted && len(prepared.drafts) == 0 && len(prepared.commands) != 0 {
			return nil, &Error{Code: CodeBadHandle, Message: "command-only save outcome is uncertain; discard and reload the aggregate"}
		}
	} else {
		prepared.signature = signature
		state.prepared = prepared
	}
	var stored []EventMessage
	err = r.enlist(ctx, func(txctx context.Context) error {
		if prepared.attempted && len(prepared.drafts) > 0 {
			existing, loadErr := r.store.Load(txctx, stream, state.initial)
			if loadErr != nil {
				return loadErr
			}
			committed, reconcileErr := kernel.ReconcilePreparedEvents(prepared.drafts, existing)
			if reconcileErr != nil {
				return reconcileErr
			}
			if committed {
				stored = append([]EventMessage(nil), existing[:len(prepared.drafts)]...)
				return nil
			}
		}
		prepared.attempted = true
		var writeErr error
		stored, writeErr = r.store.Append(txctx, stream, state.initial, prepared.drafts)
		if writeErr != nil {
			return writeErr
		}
		if len(prepared.commands) > 0 {
			if writeErr = r.options.Commands.Enqueue(txctx, prepared.commands); writeErr != nil {
				return writeErr
			}
		}
		if prepared.snapshot != nil {
			return r.store.SaveSnapshot(txctx, *prepared.snapshot)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	standAt(aggregate, prepared.version, state.tenant)
	return stored, nil
}

func (r *Repository[A]) prepareProtected(ctx context.Context, aggregate A, stream string, payloads []Payload, options SaveOptions) (*preparedAggregateSave, []byte, error) {
	state := aggregate.AggregateState()
	ids := make([]string, len(payloads))
	for index := range ids {
		ids[index] = r.options.NewID()
	}
	flow, err := messageLineage(ctx, options.Lineage)
	if err != nil {
		return nil, nil, err
	}
	batch, err := kernel.GetDraftBatch(state.id, state.initial, *r.options.SnapshotEvery, payloads, ids, flow, options.Extensions, options.Metadata, state.tenant, options.OccurredAt, state.subject)
	if err != nil {
		return nil, nil, err
	}
	prepared := &preparedAggregateSave{owner: r.store.UnitOfWork(), sealing: r.options.Sealing, version: batch.Version, drafts: make([]EventDraft, len(batch.Drafts)), commands: make([]CommandMessage, len(options.Commands))}
	for index, draft := range batch.Drafts {
		sealed, sealErr := r.options.Sealing.SealEvent(ctx, draft)
		if sealErr != nil {
			return nil, nil, sealErr
		}
		prepared.drafts[index] = *sealed
	}
	for index, command := range options.Commands {
		owned, ownErr := kernel.OwnCommand(command, state.tenant)
		if ownErr != nil {
			return nil, nil, ownErr
		}
		if owned.Subject == nil && owned.Encryption == nil {
			owned.Subject = state.Subject()
		}
		if ownErr = kernel.CheckSealedOwner(true, state.subject, owned.Subject); ownErr != nil {
			return nil, nil, ownErr
		}
		if owned.Encryption == nil {
			owned, ownErr = r.options.Sealing.SealCommand(ctx, *owned)
		} else {
			var plaintext []byte
			plaintext, ownErr = r.options.Sealing.OpenCommand(ctx, *owned)
			clear(plaintext)
		}
		if ownErr != nil {
			return nil, nil, ownErr
		}
		prepared.commands[index] = *owned
	}
	var snapshotPlaintext []byte
	if batch.Snapshot {
		snapshotPlaintext, err = r.aggregates.Dump(aggregate)
		if err != nil {
			return nil, nil, err
		}
		value, sealErr := r.options.Sealing.SealState(ctx, StateContext{Kind: "snapshot", Identity: stream, TypeName: r.aggregates.TypeName(), Version: batch.Version, Tenant: state.tenant, Subject: state.subject}, snapshotPlaintext)
		if sealErr != nil {
			return nil, nil, sealErr
		}
		prepared.snapshot = &Snapshot{AggregateID: stream, TypeName: r.aggregates.TypeName(), Version: batch.Version, Tenant: state.Tenant(), Subject: state.Subject(), Encryption: value.Encryption, Data: value.Data}
	}
	return prepared, snapshotPlaintext, nil
}
