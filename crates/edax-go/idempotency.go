package edax

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Guard claims a key and either runs next or replays the stored result.
// Results must encode as JSON. Both execution and replay return decoded JSON values:
// objects are map[string]any, arrays are []any and numbers are json.Number.
// Inside a transaction, the store must join that context's transaction for its own database.
// Any other ledger would record success before the host commits, so Guard returns ErrBadHandle.
func Guard(ctx context.Context, store IdempotencyStore, key string, hash uint64, next func(context.Context) (any, error)) (any, error) {
	if InTransaction(ctx) && !joinsOpenTransaction(ctx, store) {
		return nil, &Error{Code: CodeBadHandle, Message: "idempotency guard runs inside a transaction its ledger does not join; pass the database as UnitOfWork instead"}
	}
	return guard(ctx, store, key, hash, next, true)
}

// joinsOpenTransaction reports whether store stages its writes in a transaction open in ctx.
// Implementing TransactionalStore is not enough: a store over another database has none open.
func joinsOpenTransaction(ctx context.Context, store IdempotencyStore) bool {
	transactional, ok := store.(TransactionalStore)
	if !ok {
		return false
	}
	_, active := CurrentTransaction(ctx, transactional.UnitOfWork())
	return active
}

// GuardWithUnitOfWork commits work and its claim together when both share a database.
// With a separate ledger it commits the work before publishing the cached result.
func GuardWithUnitOfWork(ctx context.Context, store IdempotencyStore, owner UnitOfWork, key string, hash uint64, next func(context.Context) (any, error)) (value any, err error) {
	if owner == nil {
		return Guard(ctx, store, key, hash, next)
	}
	if InTransaction(ctx) {
		return nil, &Error{Code: CodeBadHandle, Message: "guard cannot open a nested transaction"}
	}
	if transactional, ok := store.(TransactionalStore); ok {
		if transactional.UnitOfWork() != owner {
			return nil, &Error{Code: CodeBadHandle, Message: "transactional ledger belongs to another database"}
		}
		err = WithinTransaction(ctx, owner, func(txctx context.Context) error {
			value, err = guard(txctx, store, key, hash, next, false)
			return err
		})
		return value, err
	}
	return Guard(ctx, store, key, hash, func(workctx context.Context) (result any, workErr error) {
		workErr = WithinTransaction(workctx, owner, func(txctx context.Context) error {
			result, workErr = next(txctx)
			if workErr == nil {
				_, workErr = doneRecord(hash, result)
			}
			return workErr
		})
		return result, workErr
	})
}

func guard(ctx context.Context, store IdempotencyStore, key string, hash uint64, next func(context.Context) (any, error), cacheFailures bool) (value any, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil || next == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "guard requires a store and handler"}
	}
	claimed, err := store.Claim(ctx, key, hash)
	if err != nil {
		return nil, err
	}
	if claimed == nil {
		return nil, &Error{Code: CodeStore, Message: "idempotency claim returned no result"}
	}
	if !claimed.Won {
		if claimed.Prior == nil {
			return nil, &Error{Code: CodeStore, Message: "lost claim has no prior record"}
		}
		prior, err := kernel.DecodePriorIdempotencyRecord(*claimed.Prior, hash)
		if err != nil {
			return nil, err
		}
		replay, err := kernel.GetIdempotencyReplay(hash, key, prior.PayloadHash, prior.State == "pending", prior.Error, prior.ErrorCode)
		if err != nil {
			return nil, err
		}
		ObserveContext(ctx, CurrentObserver(ctx), Observation{Operation: "idempotency.claim", Name: key, Outcome: replay.Outcome})
		if replay.Error != nil {
			if replay.Outcome == "failed" {
				// The kernel's replay text keeps the original code; the record keeps it apart too, and the
				// reason a rejection named beside it. A mismatch or a claim in flight is not this failure.
				return nil, kernel.Replayed(*replay.Error, prior.ErrorCode, prior.ErrorReason)
			}
			return nil, ErrorFromWire(*replay.Error)
		}
		value, decodeErr := decodeRecordResult(prior)
		if decodeErr != nil {
			return nil, &Error{Code: CodeCachedFailure, Message: decodeErr.Error()}
		}
		return value, nil
	}
	settled := false
	started := time.Now()
	outcome := "refused"
	ObserveContext(ctx, CurrentObserver(ctx), Observation{Operation: "idempotency.claim", Name: key, Outcome: "won"})
	// Cleanup must survive cancellation of the dispatch. A bounded independent
	// deadline prevents a failed remote ledger from holding the caller forever.
	defer func() {
		if !settled {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			releaseErr := store.Release(cleanup, key, claimed.Handle)
			if releaseErr == nil {
				outcome = "released"
			}
			err = errors.Join(err, releaseErr)
		}
		if settled && joinsOpenTransaction(ctx, store) {
			outcome = "staged"
		}
		ObserveContext(ctx, CurrentObserver(ctx), Observation{Operation: "idempotency.settle", Name: key, Outcome: outcome, Duration: time.Since(started), Err: err})
	}()
	value, err = next(ctx)
	if err == nil {
		err = ctx.Err()
	}
	var record *IdempotencyRecord
	if err == nil {
		record, err = doneRecord(hash, value)
		if err == nil {
			value, err = decodeRecordResult(record)
		}
	}
	if err != nil {
		if cacheFailures && ctx.Err() == nil {
			cached, decisionErr := kernel.IdempotencySettleFailure(hash, errorMessage(err), errorCode(err), errorReason(err))
			if decisionErr != nil {
				return nil, errors.Join(err, decisionErr)
			}
			if cached != nil {
				cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				if completeErr := store.Complete(cleanup, key, claimed.Handle, *cached); completeErr == nil {
					settled = true
					outcome = "cached"
				} else {
					err = errors.Join(err, completeErr)
				}
			}
		}
		return nil, err
	}
	if err = store.Complete(ctx, key, claimed.Handle, *record); err != nil {
		return nil, err
	}
	settled = true
	outcome = "completed"
	return value, nil
}

func decodeRecordResult(record *IdempotencyRecord) (any, error) {
	if record.ResultJSON == nil {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewBufferString(*record.ResultJSON))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, WrapError(CodeCodec, err.Error(), err)
	}
	return value, nil
}

func doneRecord(hash uint64, value any) (*IdempotencyRecord, error) {
	encoded, err := encodeJSON(value)
	if err != nil {
		return nil, WrapError(CodeCodec, err.Error(), err)
	}
	raw := string(encoded)
	record := &IdempotencyRecord{PayloadHash: hash, State: "done", ResultJSON: &raw}
	if _, err = EncodeRecord(*record); err != nil {
		return nil, err
	}
	return record, nil
}

// IdempotentOptions fixes the ledger namespace and optional transaction boundary.
type IdempotentOptions struct {
	Scope      string
	Tenancy    Tenancy
	UnitOfWork UnitOfWork
	// Codec decodes and upcasts the payload before the kernel hashes its JSON
	// value. Pass the handler's codec whenever it uses MessagePack or upcasts;
	// without it the guard hashes the raw bytes.
	Codec PayloadCodec
}

// Idempotent hashes the command name and JSON payload in the kernel.
func Idempotent(store IdempotencyStore, options IdempotentOptions) (Middleware, error) {
	if store == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "idempotency requires a store"}
	}
	mode := options.Tenancy
	if mode == "" {
		mode = TenancySilo
	}
	if _, err := kernel.GetTenancyRules(string(mode)); err != nil {
		return nil, err
	}
	return func(next CommandHandler) CommandHandler {
		return func(ctx context.Context, message CommandMessage) (any, error) {
			key := message.IdempotencyKey
			if dispatch, ok := ctx.Value(commandContextKey{}).(commandDispatch); ok && dispatch.received {
				received, err := kernel.ReceivedKey(message.ID, key)
				if err != nil {
					return nil, err
				}
				key = &received
			}
			if key == nil {
				return next(ctx, message)
			}
			ledgerKey, err := kernel.ClaimKey(string(mode), options.Scope, *key, message.Tenant)
			if err != nil {
				return nil, err
			}
			payload := message.Data
			if options.Codec != nil {
				value, decodeErr := options.Codec.Unmarshal(Payload{Name: message.Name, Version: message.Version, Data: message.Data})
				if decodeErr != nil {
					return nil, decodeErr
				}
				payload, decodeErr = encodeJSON(value)
				if decodeErr != nil {
					return nil, WrapError(CodeCodec, decodeErr.Error(), decodeErr)
				}
			}
			hash, err := kernel.CommandHashJSON(message.Name, payload)
			if err != nil {
				return nil, err
			}
			return GuardWithUnitOfWork(ctx, store, options.UnitOfWork, ledgerKey, hash, func(runctx context.Context) (any, error) { return next(runctx, message) })
		}
	}, nil
}
