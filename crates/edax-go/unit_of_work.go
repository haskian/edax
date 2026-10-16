package edax

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Tx is one backend transaction. It must reject calls after settlement.
type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// UnitOfWork owns a database transaction boundary. Implementations must be comparable pointers.
type UnitOfWork interface {
	Begin(ctx context.Context) (Tx, error)
}

type transactionKey struct{ owner UnitOfWork }
type transactionMarker struct{}
type transactionRootResources struct{}
type transactionCurrentResources struct{}

// InTransaction reports whether ctx belongs to any open unit of work.
func InTransaction(ctx context.Context) bool { return ctx.Value(transactionMarker{}) != nil }

// CurrentTransaction returns the transaction for this database only.
func CurrentTransaction(ctx context.Context, owner UnitOfWork) (Tx, bool) {
	tx, ok := ctx.Value(transactionKey{owner}).(Tx)
	return tx, ok
}

type transactionResourceKey struct{ owner UnitOfWork }
type transactionResources struct {
	mu      sync.Mutex
	release []func()
	guards  []*sealingGuard
	settled atomic.Bool
	parent  *transactionResources
}

func checkTransactionResources(ctx context.Context) error {
	for resources, _ := ctx.Value(transactionCurrentResources{}).(*transactionResources); resources != nil; resources = resources.parent {
		if resources.settled.Load() {
			return &Error{Code: CodeBadHandle, Message: "cannot use a settled transaction context"}
		}
	}
	return nil
}

// HoldTransactionResource retains a scope fence until commit or rollback completes.
// Returns false when ctx has no managed transaction for owner; caller then owns release.
func HoldTransactionResource(ctx context.Context, owner UnitOfWork, release func()) bool {
	resources, ok := ctx.Value(transactionResourceKey{owner}).(*transactionResources)
	if !ok {
		return false
	}
	resources.mu.Lock()
	if checkTransactionResources(ctx) != nil {
		resources.mu.Unlock()
		return false
	}
	resources.release = append(resources.release, release)
	resources.mu.Unlock()
	return true
}

// WithinTransaction publishes a transaction through ctx and rolls it back on failure or panic.
// A second transaction on the same database is refused; callers should join the existing ctx.
func WithinTransaction(ctx context.Context, owner UnitOfWork, run func(context.Context) error) (err error) {
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = checkTransactionResources(ctx); err != nil {
		return err
	}
	if owner == nil {
		return &Error{Code: CodeBadHandle, Message: "transaction requires a database"}
	}
	if _, ok := CurrentTransaction(ctx, owner); ok {
		return &Error{Code: CodeBadHandle, Message: "nested transaction on the same database"}
	}
	tx, err := owner.Begin(ctx)
	if err != nil {
		return err
	}
	parent, _ := ctx.Value(transactionCurrentResources{}).(*transactionResources)
	resources := &transactionResources{parent: parent}
	defer func() {
		resources.mu.Lock()
		resources.settled.Store(true)
		releases := resources.release
		resources.release = nil
		resources.mu.Unlock()
		for index := len(releases) - 1; index >= 0; index-- {
			releases[index]()
		}
	}()
	ctx = context.WithValue(ctx, transactionResourceKey{owner}, resources)
	ctx = context.WithValue(ctx, transactionCurrentResources{}, resources)
	if ctx.Value(transactionRootResources{}) == nil {
		ctx = context.WithValue(ctx, transactionRootResources{}, resources)
	}
	committed := false
	started := time.Now()
	defer func() {
		outcome := "rolled_back"
		if committed {
			outcome = "committed"
		}
		ObserveContext(ctx, CurrentObserver(ctx), Observation{Operation: "transaction", Outcome: outcome, Duration: time.Since(started), Err: err})
	}()
	defer func() {
		if !committed {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			err = errors.Join(err, tx.Rollback(cleanup))
		}
	}()
	ctx = context.WithValue(ctx, transactionMarker{}, true)
	if err = run(context.WithValue(ctx, transactionKey{owner}, tx)); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

// WithoutTransaction keeps cancellation and request values while removing database transactions.
// Long-lived workers must not inherit a transaction that their caller will close.
func WithoutTransaction(ctx context.Context) context.Context { return detachedTransactions{ctx} }

type detachedTransactions struct{ context.Context }

// Value hides transaction ownership while retaining other context values.
func (ctx detachedTransactions) Value(key any) any {
	switch key.(type) {
	case transactionKey, transactionMarker, transactionResourceKey, transactionRootResources, transactionCurrentResources, sealingGuardKey, openedEventKey, authenticatedEventKey:
		return nil
	default:
		return ctx.Context.Value(key)
	}
}

// HoldCurrentTransactionResource retains a guard through the outermost open transaction.
func HoldCurrentTransactionResource(ctx context.Context, release func()) bool {
	resources, ok := ctx.Value(transactionRootResources{}).(*transactionResources)
	if !ok {
		return false
	}
	resources.mu.Lock()
	if checkTransactionResources(ctx) != nil {
		resources.mu.Unlock()
		return false
	}
	resources.release = append(resources.release, release)
	resources.mu.Unlock()
	return true
}
