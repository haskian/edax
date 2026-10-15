package edax

import (
	"bytes"
	"context"
	"encoding/hex"
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
			cached, decisionErr := kernel.IdempotencySettleFailure(hash, errorMessage(err), errorCode(err))
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
			dispatch, dispatched := ctx.Value(commandContextKey{}).(commandDispatch)
			if dispatched && dispatch.received {
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
			if dispatch.sealing != nil {
				payload, err = dispatch.openedPayload(message)
				if err != nil {
					return nil, err
				}
			}
			if options.Codec != nil {
				value, decodeErr := options.Codec.Unmarshal(Payload{Name: message.Name, Version: message.Version, Data: payload})
				if decodeErr != nil {
					return nil, decodeErr
				}
				payload, decodeErr = encodeJSON(value)
				if decodeErr != nil {
					return nil, WrapError(CodeCodec, decodeErr.Error(), decodeErr)
				}
			}
			if dispatch.sealing != nil {
				return guardProtectedCommand(ctx, store, options.UnitOfWork, dispatch.sealing, message, payload, ledgerKey, next)
			}
			hash, err := kernel.CommandHashJSON(message.Name, payload)
			if err != nil {
				return nil, err
			}
			return GuardWithUnitOfWork(ctx, store, options.UnitOfWork, ledgerKey, hash, func(runctx context.Context) (any, error) { return next(runctx, message) })
		}
	}, nil
}

// The ordinary guard still owns claiming, retries, and transaction settlement.
// Its storage hash is always zero; only the protected record carries a fingerprint.
type protectedLedger struct {
	store       IdempotencyStore
	sealing     *Sealing
	scope       KeyScope
	fingerprint string
	claims      map[protectedClaimKey]*protectedClaim
}

type protectedClaimKey struct{ key, handle string }
type protectedClaim struct{ pending, owner, settled string }

// Only the wrapper over an actual transactional store advertises that capability.
type transactionalProtectedLedger struct {
	*protectedLedger
	owner UnitOfWork
}

func (s *transactionalProtectedLedger) UnitOfWork() UnitOfWork { return s.owner }

func guardProtectedCommand(ctx context.Context, store IdempotencyStore, owner UnitOfWork, sealing *Sealing, message CommandMessage, payload []byte, key string, next CommandHandler) (any, error) {
	scope, err := kernel.ProtectedScope(sealing.KeyNamespace, message.Tenant, message.Subject)
	if err != nil {
		return nil, err
	}
	ctx, release, err := sealing.Guard(ctx, *scope)
	if err != nil {
		return nil, err
	}
	resourceOwner := owner
	if resourceOwner == nil {
		if transactional, ok := store.(TransactionalStore); ok {
			resourceOwner = transactional.UnitOfWork()
		}
	}
	if resourceOwner == nil || !HoldTransactionResource(ctx, resourceOwner, release) {
		defer release()
	}
	reserved, err := sealing.Keys.Fingerprint(ctx, *scope)
	if err != nil {
		clear(reserved.Key)
		return nil, err
	}
	fingerprint, err := kernel.ProtectedFingerprint(message.Name, message.Version, payload, reserved.KeyID, reserved.Key)
	clear(reserved.Key)
	if err != nil {
		return nil, err
	}
	adapter := &protectedLedger{store: store, sealing: sealing, scope: *scope, fingerprint: fingerprint, claims: make(map[protectedClaimKey]*protectedClaim)}
	var protected IdempotencyStore = adapter
	if transactional, ok := store.(TransactionalStore); ok {
		protected = &transactionalProtectedLedger{protectedLedger: adapter, owner: transactional.UnitOfWork()}
	}
	return GuardWithUnitOfWork(ctx, protected, owner, key, 0, func(runctx context.Context) (any, error) { return next(runctx, message) })
}

func (s *protectedLedger) checkActive(ctx context.Context) error {
	status, err := s.sealing.Keys.Status(ctx, s.scope)
	if err != nil {
		return err
	}
	return kernel.CheckKeyStatus(status)
}

func (s *protectedLedger) Claim(ctx context.Context, key string, hash uint64) (*ClaimResult, error) {
	if hash != 0 {
		return nil, &Error{Code: CodeBadHandle, Message: "protected ledger requires the constant guard hash"}
	}
	if err := s.checkActive(ctx); err != nil {
		return nil, err
	}
	storedKey, err := kernel.ProtectedClaimKey(s.scope, key)
	if err != nil {
		return nil, err
	}
	claimed, err := s.store.Claim(ctx, storedKey, 0)
	if err != nil {
		return nil, err
	}
	if claimed == nil {
		return nil, &Error{Code: CodeStore, Message: "idempotency claim returned no result"}
	}
	if claimed.Won {
		owner := hex.EncodeToString(claimed.Handle)
		if owner == "" {
			owner = Guid()
		}
		pending, err := kernel.ProtectedPending(s.scope, storedKey, owner, s.fingerprint)
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			return nil, errors.Join(err, s.store.Release(cleanup, storedKey, claimed.Handle))
		}
		s.claims[protectedClaimKey{storedKey, string(claimed.Handle)}] = &protectedClaim{pending: pending, owner: owner}
		return claimed, nil
	}
	if claimed.Prior == nil {
		return nil, &Error{Code: CodeStore, Message: "lost protected claim has no prior record"}
	}
	prior, err := kernel.DecodePriorIdempotencyRecord(*claimed.Prior, 0)
	if err != nil {
		return nil, err
	}
	if prior.PayloadHash != 0 {
		return nil, &Error{Code: CodeCodec, Message: "invalid protected ledger envelope"}
	}
	if prior.State == "pending" {
		return protectedPrior(IdempotencyRecord{PayloadHash: 0, State: "pending"})
	}
	if prior.State != "done" || prior.ResultJSON == nil {
		return nil, &Error{Code: CodeCodec, Message: "invalid protected ledger envelope"}
	}
	var record string
	if err := json.Unmarshal([]byte(*prior.ResultJSON), &record); err != nil {
		return nil, &Error{Code: CodeCodec, Message: "invalid protected ledger envelope"}
	}
	replay, err := kernel.GetProtectedReplay(record, s.scope, storedKey, s.fingerprint)
	if err != nil {
		return nil, err
	}
	if replay == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "missing protected replay decision"}
	}
	switch replay.Action {
	case "pending":
		return protectedPrior(IdempotencyRecord{PayloadHash: 0, State: "pending"})
	case "failed":
		if replay.Code == nil {
			return nil, &Error{Code: CodeCodec, Message: "missing protected failure code"}
		}
		if err := s.checkActive(ctx); err != nil {
			return nil, err
		}
		safe := "protected command failed"
		return protectedPrior(IdempotencyRecord{PayloadHash: 0, State: "failed", Error: &safe, ErrorCode: replay.Code})
	case "done":
		if replay.Encryption == nil {
			return nil, &Error{Code: CodeCodec, Message: "missing protected result encryption"}
		}
		secret, err := s.sealing.resolve(ctx, s.scope.Tenant, s.scope.Subject, *replay.Encryption)
		defer clear(secret)
		if err != nil {
			return nil, err
		}
		plaintext, err := kernel.ProtectedOpen(record, secret)
		if err != nil {
			return nil, err
		}
		raw := string(plaintext)
		if err := s.checkActive(ctx); err != nil {
			return nil, err
		}
		return protectedPrior(IdempotencyRecord{PayloadHash: 0, State: "done", ResultJSON: &raw})
	default:
		return nil, &Error{Code: CodeBadHandle, Message: "unknown protected replay decision"}
	}
}

func protectedPrior(record IdempotencyRecord) (*ClaimResult, error) {
	raw, err := EncodeRecord(record)
	if err != nil {
		return nil, err
	}
	return &ClaimResult{Won: false, Prior: &raw}, nil
}

func (s *protectedLedger) Complete(ctx context.Context, key string, handle []byte, record IdempotencyRecord) error {
	if record.PayloadHash != 0 {
		return &Error{Code: CodeBadHandle, Message: "protected ledger requires the constant guard hash"}
	}
	storedKey, err := kernel.ProtectedClaimKey(s.scope, key)
	if err != nil {
		return err
	}
	claim := s.claims[protectedClaimKey{storedKey, string(handle)}]
	if claim == nil {
		return &Error{Code: CodeBadHandle, Message: "protected settlement has no local claim owner"}
	}
	if err := s.checkActive(ctx); err != nil {
		return err
	}
	if claim.settled == "" {
		switch record.State {
		case "done":
			if _, err := EncodeRecord(record); err != nil {
				return err
			}
			plaintext := []byte("null")
			if record.ResultJSON != nil {
				plaintext = []byte(*record.ResultJSON)
			}
			info, secret, err := s.sealing.prepare(ctx, s.scope.Tenant, s.scope.Subject)
			if err != nil {
				clear(secret)
				return err
			}
			claim.settled, err = kernel.ProtectedFinish(claim.pending, claim.owner, info, secret, plaintext)
			clear(secret)
			if err != nil {
				return err
			}
		case "failed":
			if record.ErrorCode == nil {
				return &Error{Code: CodeBadHandle, Message: "protected failures require a stable error code"}
			}
			claim.settled, err = kernel.ProtectedFailure(claim.pending, claim.owner, *record.ErrorCode)
			if err != nil {
				return err
			}
		default:
			return &Error{Code: CodeBadHandle, Message: "protected settlement requires a completed outcome"}
		}
	}
	outer, err := doneRecord(0, claim.settled)
	if err != nil {
		return err
	}
	return s.store.Complete(ctx, storedKey, handle, *outer)
}

func (s *protectedLedger) Release(ctx context.Context, key string, handle []byte) error {
	storedKey, err := kernel.ProtectedClaimKey(s.scope, key)
	if err != nil {
		return err
	}
	if err := s.store.Release(ctx, storedKey, handle); err != nil {
		return err
	}
	delete(s.claims, protectedClaimKey{storedKey, string(handle)})
	return nil
}

func (s *protectedLedger) Forget(ctx context.Context, key string) error {
	storedKey, err := kernel.ProtectedClaimKey(s.scope, key)
	if err != nil {
		return err
	}
	return s.store.Forget(ctx, storedKey)
}
