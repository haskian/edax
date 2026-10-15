package edax

import (
	"context"
	"encoding/json"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// SagaInstance holds committed state, version and folded-event marks. Completed rows are tombstones.
// Protected replaces State for encrypted instances; handlers receive authenticated plaintext.
type SagaInstance struct {
	Version   uint64
	Completed bool
	State     json.RawMessage
	Folded    string
	Protected *ProtectedSagaState
}

// ProtectedSagaState carries encrypted application state without interpreting it as domain JSON.
type ProtectedSagaState = kernel.ProtectedState

// SagaStore atomically writes the state, folded marks and commands at an expected version.
// A missing instance is represented by nil. Completed instances remain as tombstones.
// Save's state is an opaque serialized value: protected runners pass the kernel's encrypted envelope.
type SagaStore interface {
	UnitOfWork() UnitOfWork
	Load(ctx context.Context, sagaName string, instanceID string) (*SagaInstance, error)
	Save(ctx context.Context, sagaName string, instanceID string, state json.RawMessage, expectedVersion uint64, foldedMarksJSON string, commands []CommandMessage) (uint64, error)
	Complete(ctx context.Context, sagaName string, instanceID string, expectedVersion uint64, commands []CommandMessage) (uint64, error)
	Unpublished(ctx context.Context, sagaName string, limit uint64) ([]CommandMessage, error)
	MarkPublished(ctx context.Context, sagaName string, ids []string) error
}

// SagaStep contains the next state and already encoded commands. Complete creates a tombstone.
type SagaStep struct {
	State    json.RawMessage
	Commands []Payload
	Complete bool
}

// Saga supplies domain state transitions. Implementations may be called concurrently for different instances.
type Saga interface {
	Name() string
	InstanceID(event EventMessage) (string, bool)
	Handle(ctx context.Context, state json.RawMessage, event EventMessage) (SagaStep, error)
}

// SagaOptions supplies observability, a clock, optional sealing and a callback for kernel-refused events.
type SagaOptions struct {
	Observer  Observer
	Now       func() int64
	OnSkipped func(saga string, action string, eventID string)
	Sealing   *Sealing
}

// SagaRunner coordinates kernel decisions and atomic store writes for a Saga.
// It borrows the store and does not close it.
type SagaRunner struct {
	saga    Saga
	store   SagaStore
	options SagaOptions
}

// NewSagaRunner validates dependencies and uses the kernel clock unless Now is supplied.
func NewSagaRunner(saga Saga, store SagaStore, options SagaOptions) (*SagaRunner, error) {
	if saga == nil || store == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "saga runner requires a saga and store"}
	}
	if options.Now == nil {
		options.Now = CurrentTimestamp
	}
	return &SagaRunner{saga: saga, store: store, options: options}, nil
}

// OnEvent folds an event in the existing store transaction, or opens and commits its own.
func (r *SagaRunner) OnEvent(ctx context.Context, message EventMessage) error {
	raw := originalEvent(ctx, message)
	if sealing := r.options.Sealing; sealing != nil {
		var data []byte
		var err error
		skip := false
		if cached, ok := ctx.Value(authenticatedEventKey{}).(authenticatedEvent); ok && cached.sealing == sealing {
			data, err = cached.openedPayload(raw)
		} else {
			data, skip, err = sealing.OpenEventDelivery(ctx, raw)
		}
		if err != nil || skip {
			return err
		}
		message, err = openedEventView(raw, data)
		if err != nil {
			return err
		}
	} else if raw.Encryption != nil {
		return &Error{Code: CodeBadHandle, Message: "encrypted saga events require configured sealing"}
	}
	return r.guarded(ctx, raw.Tenant, raw.Subject, func(ctx context.Context) error {
		id, ok := r.saga.InstanceID(message)
		if !ok {
			return nil
		}
		handlerContext, err := CausedBy(ctx, message.ID, message.CorrelationID)
		if err != nil {
			return err
		}
		return r.fold(handlerContext, id, message, raw)
	})
}

// CommandFailed converts a failed saga command into the kernel failure event and folds it.
func (r *SagaRunner) CommandFailed(ctx context.Context, dead DeadLetter[CommandMessage]) error {
	if sealing := r.options.Sealing; sealing != nil {
		_, skip, err := sealing.OpenCommandDelivery(ctx, dead.Message)
		if err != nil || skip {
			return err
		}
	} else if dead.Message.Encryption != nil {
		return &Error{Code: CodeBadHandle, Message: "encrypted saga commands require configured sealing"}
	}
	return r.guarded(ctx, dead.Message.Tenant, dead.Message.Subject, func(ctx context.Context) error {
		message, err := kernel.SagaCommandFailed(dead.Message, errorMessage(dead.Failure), r.options.Now(), errorCode(dead.Failure))
		if err != nil {
			return err
		}
		handlerContext, err := CausedBy(ctx, message.ID, message.CorrelationID)
		if err != nil {
			return err
		}
		return r.fold(handlerContext, message.AggregateID, *message, *message)
	})
}

func (r *SagaRunner) guarded(ctx context.Context, tenant, subject *string, run func(context.Context) error) error {
	sealing := r.options.Sealing
	if sealing == nil {
		return run(ctx)
	}
	scope, err := kernel.ProtectedScope(sealing.KeyNamespace, tenant, subject)
	if err != nil {
		return err
	}
	guarded, release, err := sealing.Guard(ctx, *scope)
	if err != nil {
		if destroyedKey(err) {
			return nil
		}
		return err
	}
	if !HoldTransactionResource(guarded, r.store.UnitOfWork(), release) {
		defer release()
	}
	return run(guarded)
}

func (r *SagaRunner) fold(ctx context.Context, id string, message, raw EventMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	owner := r.store.UnitOfWork()
	if owner == nil {
		return &Error{Code: CodeBadHandle, Message: "saga store requires a transaction boundary"}
	}
	if _, ok := CurrentTransaction(ctx, owner); ok {
		return r.foldIn(ctx, id, message, raw)
	}
	return WithinTransaction(ctx, owner, func(ctx context.Context) error { return r.foldIn(ctx, id, message, raw) })
}
func (r *SagaRunner) foldIn(ctx context.Context, id string, message, raw EventMessage) error {
	instance, err := r.store.Load(ctx, r.saga.Name(), id)
	if err != nil {
		return err
	}
	var version *uint64
	var state json.RawMessage
	completed, folded := false, ""
	if instance != nil {
		version = &instance.Version
		state = instance.State
		completed = instance.Completed
		folded = instance.Folded
	}
	sealing := r.options.Sealing
	if instance != nil {
		protected := instance.Protected
		if protected == nil && len(state) != 0 {
			decoded, err := kernel.DecodeProtectedState(string(state))
			if err != nil {
				return err
			}
			protected = decoded.Value
		}
		if protected != nil {
			if instance.Completed && !protected.Completed {
				return &Error{Code: CodeCodec, Message: "saga completion disagrees with authenticated state"}
			}
			if sealing == nil {
				return &Error{Code: CodeBadHandle, Message: "protected saga state requires configured sealing"}
			}
			if err := kernel.CheckSealedOwner(true, protected.Tenant, raw.Tenant); err != nil {
				return err
			}
			if err := kernel.CheckSealedOwner(true, protected.Subject, raw.Subject); err != nil {
				return err
			}
			completed = protected.Completed
			state, err = sealing.OpenState(ctx, StateContext{Kind: "saga", Identity: id, TypeName: r.saga.Name(), Version: instance.Version, Tenant: protected.Tenant, Subject: protected.Subject, Marks: folded, Completed: completed}, protected.Encryption, protected.Data)
			if err != nil {
				return err
			}
		} else if sealing != nil {
			if err := sealing.CheckLegacy(ctx, raw.Tenant, raw.Subject); err != nil {
				return err
			}
		}
	}
	admit, err := kernel.GetSagaAdmit(r.saga.Name(), completed, folded, raw, version)
	if err != nil {
		return err
	}
	switch admit.Action {
	case "completed", "repeat", "below_mark":
		observer := r.options.Observer
		if observer == nil {
			observer = CurrentObserver(ctx)
		}
		ObserveContext(ctx, observer, Observation{Operation: "saga", Name: r.saga.Name(), ID: message.ID, CorrelationID: message.CorrelationID, Outcome: admit.Action})
		if r.options.OnSkipped != nil {
			r.options.OnSkipped(r.saga.Name(), admit.Action, message.ID)
		}
		return nil
	case "fold":
	default:
		return invalidDecision("saga admit action " + admit.Action)
	}
	step, err := r.saga.Handle(ctx, state, message)
	if err != nil {
		return err
	}
	write, err := kernel.GetSagaWrite(step.Complete, len(step.Commands) > 0, completed, version)
	if err != nil {
		return err
	}
	if write.Action == "nothing" {
		return nil
	}
	if write.Expected == nil || write.Version == nil {
		return invalidDecision("saga write version")
	}
	rows := make([]CommandMessage, 0, len(step.Commands))
	issued := r.options.Now()
	for index, payload := range step.Commands {
		row, err := kernel.SagaCommand(r.saga.Name(), id, *write.Version, uint64(index), payload, message.ID, message.CorrelationID, issued, message.Tenant, message.Subject)
		if err != nil {
			return err
		}
		if sealing != nil {
			row, err = sealing.SealCommand(ctx, *row)
			if err != nil {
				return err
			}
		}
		rows = append(rows, *row)
	}
	switch write.Action {
	case "save":
		if admit.Folded == nil {
			return invalidDecision("saga folded marks")
		}
		if len(step.State) == 0 {
			step.State = json.RawMessage("null")
		}
		if sealing != nil {
			sealed, err := sealing.SealState(ctx, StateContext{Kind: "saga", Identity: id, TypeName: r.saga.Name(), Version: *write.Version, Tenant: raw.Tenant, Subject: raw.Subject, Marks: *admit.Folded}, step.State)
			if err != nil {
				return err
			}
			encoded, err := kernel.EncodeProtectedState(ProtectedSagaState{Tenant: raw.Tenant, Subject: raw.Subject, Encryption: sealed.Encryption, Data: sealed.Data})
			if err != nil {
				return err
			}
			step.State = json.RawMessage(encoded)
		}
		_, err = r.store.Save(ctx, r.saga.Name(), id, step.State, *write.Expected, *admit.Folded, rows)
	case "complete":
		if sealing == nil {
			_, err = r.store.Complete(ctx, r.saga.Name(), id, *write.Expected, rows)
		} else {
			if admit.Folded == nil {
				return invalidDecision("saga folded marks")
			}
			sealed, sealErr := sealing.SealState(ctx, StateContext{Kind: "saga", Identity: id, TypeName: r.saga.Name(), Version: *write.Version, Tenant: raw.Tenant, Subject: raw.Subject, Marks: *admit.Folded, Completed: true}, []byte("null"))
			if sealErr != nil {
				return sealErr
			}
			encoded, encodeErr := kernel.EncodeProtectedState(ProtectedSagaState{Tenant: raw.Tenant, Subject: raw.Subject, Encryption: sealed.Encryption, Data: sealed.Data, Completed: true})
			if encodeErr != nil {
				return encodeErr
			}
			_, err = r.store.Save(ctx, r.saga.Name(), id, json.RawMessage(encoded), *write.Expected, *admit.Folded, rows)
		}
	default:
		return invalidDecision("saga write action " + write.Action)
	}
	return err
}

type sagaOutbox struct {
	store SagaStore
	name  string
}

// Unpublished reads committed commands for this saga namespace.
func (o sagaOutbox) Unpublished(ctx context.Context, limit uint64) ([]CommandMessage, error) {
	return o.store.Unpublished(ctx, o.name, limit)
}

// MarkPublished settles command outbox rows in this saga namespace.
func (o sagaOutbox) MarkPublished(ctx context.Context, ids []string) error {
	return o.store.MarkPublished(ctx, o.name, ids)
}

// Outbox exposes committed commands for this saga as a relay source.
func (r *SagaRunner) Outbox() Outbox[CommandMessage] {
	return sagaOutbox{store: r.store, name: r.saga.Name()}
}

// PublishTo executes saga commands through bus and returns its execution error.
func (r *SagaRunner) PublishTo(bus *CommandBus) Publish[CommandMessage] {
	return func(ctx context.Context, message CommandMessage) error {
		_, _, err := bus.Deliver(ctx, message)
		return err
	}
}
