package edax_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
)

type cleanupOwner struct{ tx *cleanupTx }

func (o *cleanupOwner) Begin(context.Context) (edax.Tx, error) { return o.tx, nil }

type cleanupTx struct {
	deadline bool
	canceled bool
	commits  int
}

func (t *cleanupTx) Commit(context.Context) error { t.commits++; return nil }
func (t *cleanupTx) Rollback(ctx context.Context) error {
	limit, ok := ctx.Deadline()
	t.deadline = ok && time.Until(limit) > 0 && time.Until(limit) <= 5*time.Second
	t.canceled = ctx.Err() != nil
	return nil
}
func TestRollbackHasIndependentDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := new(cleanupTx)
	err := edax.WithinTransaction(ctx, &cleanupOwner{tx}, func(context.Context) error { cancel(); return context.Canceled })
	if !errors.Is(err, context.Canceled) || !tx.deadline || tx.canceled || tx.commits != 0 {
		t.Fatalf("error %v cleanup %+v", err, tx)
	}
}

func TestTransactionObserverPanicUsesContextLogger(t *testing.T) {
	var logs bytes.Buffer
	ctx := edax.WithLogger(context.Background(), slog.New(slog.NewTextHandler(&logs, nil)).With("service", "billing"))
	ctx = edax.WithObserver(ctx, edax.ObserverFunc(func(edax.Observation) { panic("metrics exporter unavailable") }))
	if err := edax.WithinTransaction(ctx, &cleanupOwner{new(cleanupTx)}, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if output := logs.String(); !strings.Contains(output, "service=billing") || !strings.Contains(output, "operation=transaction") {
		t.Fatalf("context logger missed observer panic: %s", output)
	}
}

type malformedProjection struct{ edax.ProjectionStore }

func (malformedProjection) Get(context.Context, string) (*edax.ProjectionRow, error) {
	return &edax.ProjectionRow{Version: 1, DocumentJSON: `{"id":"invoice-1042","amount":"12.00"} {"id":"unexpected"}`}, nil
}
func TestProjectionRejectsTrailingJSON(t *testing.T) {
	_, err := edax.LoadEntity(context.Background(), malformedProjection{}, "invoice-1042")
	if !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("malformed document accepted: %v", err)
	}
}

// keyedDatabase holds idempotency keys behind transaction boundaries that fail on demand. A key that a
// transaction claims stays held until the transaction settles, as a unique index holds it, so a scope
// that never rolls back keeps its key from every later claimant.
type keyedDatabase struct {
	mu                               sync.Mutex
	committed                        map[string]string
	held                             map[string]bool
	beginErr, commitErr, rollbackErr error
	begins, commits, rollbacks       int
}

type keyedTx struct {
	database *keyedDatabase
	staged   map[string]string
}

func newKeyedDatabase() *keyedDatabase {
	return &keyedDatabase{committed: map[string]string{}, held: map[string]bool{}}
}

func (d *keyedDatabase) Begin(context.Context) (edax.Tx, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.begins++
	if d.beginErr != nil {
		return nil, d.beginErr
	}
	return &keyedTx{database: d, staged: map[string]string{}}, nil
}

// Commit applies nothing when commitErr is set: the database lost the transaction.
func (tx *keyedTx) Commit(context.Context) error {
	d := tx.database
	d.mu.Lock()
	defer d.mu.Unlock()
	d.commits++
	if d.commitErr != nil {
		return d.commitErr
	}
	for key, record := range tx.staged {
		d.committed[key] = record
		delete(d.held, key)
	}
	tx.staged = map[string]string{}
	return nil
}

// Rollback releases the held keys even when it reports rollbackErr, as a dropped connection does.
func (tx *keyedTx) Rollback(context.Context) error {
	d := tx.database
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rollbacks++
	for key := range tx.staged {
		delete(d.held, key)
	}
	tx.staged = map[string]string{}
	return d.rollbackErr
}

// claim stages key in the context's transaction and refuses a key committed or held by another one.
func (d *keyedDatabase) claim(ctx context.Context, key, record string) error {
	active, ok := edax.CurrentTransaction(ctx, d)
	if !ok {
		return &edax.Error{Code: edax.CodeBadHandle, Message: "claim outside a transaction"}
	}
	tx := active.(*keyedTx)
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, done := d.committed[key]; done || d.held[key] {
		return &edax.Error{Code: edax.CodeConcurrency, Message: "key " + key + " is taken"}
	}
	d.held[key] = true
	tx.staged[key] = record
	return nil
}

func TestWithinTransactionSettlesEveryFailure(t *testing.T) {
	const key = "billing:charge:1042"
	const record = `{"invoice_id":"1042","total":"49.90","currency":"EUR"}`
	errBegin := errors.New("connection pool exhausted")
	errHandler := errors.New("invoice 1042 is already paid")
	errCommit := errors.New("commit lost: connection reset")
	errRollback := errors.New("rollback lost: connection reset")
	crash := errors.New("payment gateway client crashed")
	returns := func(err error) func(context.CancelFunc) error {
		return func(context.CancelFunc) error { return err }
	}
	for _, test := range []struct {
		name                             string
		beginErr, commitErr, rollbackErr error
		// after runs once the body has claimed the key; nil returns success.
		after              func(cancel context.CancelFunc) error
		want               []error
		panics             bool
		commits, rollbacks int
		committed          bool
	}{
		{name: "body_succeeds", commits: 1, committed: true},
		{name: "begin_fails", beginErr: errBegin, want: []error{errBegin}},
		{name: "body_errors", after: returns(errHandler), want: []error{errHandler}, rollbacks: 1},
		{name: "body_panics", after: func(context.CancelFunc) error { panic(crash) }, panics: true, rollbacks: 1},
		{name: "commit_fails", commitErr: errCommit, want: []error{errCommit}, commits: 1, rollbacks: 1},
		{name: "rollback_fails_after_body_error", rollbackErr: errRollback, after: returns(errHandler), want: []error{errHandler, errRollback}, rollbacks: 1},
		{name: "rollback_fails_after_panic", rollbackErr: errRollback, after: func(context.CancelFunc) error { panic(crash) }, panics: true, rollbacks: 1},
		{name: "context_cancelled_after_successful_body", after: func(cancel context.CancelFunc) error { cancel(); return nil }, want: []error{context.Canceled}, rollbacks: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newKeyedDatabase()
			database.beginErr, database.commitErr, database.rollbackErr = test.beginErr, test.commitErr, test.rollbackErr
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ran := false
			var err error
			recovered := func() (recovered any) {
				defer func() { recovered = recover() }()
				err = edax.WithinTransaction(ctx, database, func(txctx context.Context) error {
					ran = true
					if claimErr := database.claim(txctx, key, record); claimErr != nil {
						return claimErr
					}
					if test.after == nil {
						return nil
					}
					return test.after(cancel)
				})
				return nil
			}()
			if test.panics != (recovered != nil) || (test.panics && recovered != crash) {
				t.Fatalf("panic %v, want the body's own panic: %v", recovered, test.panics)
			}
			for _, want := range test.want {
				if !errors.Is(err, want) {
					t.Fatalf("error %v lost %v", err, want)
				}
			}
			if len(test.want) == 0 && err != nil {
				t.Fatalf("error %v", err)
			}
			if ran != (test.beginErr == nil) {
				t.Fatalf("body ran=%v after begin error %v", ran, test.beginErr)
			}
			if database.begins != 1 || database.commits != test.commits || database.rollbacks != test.rollbacks {
				t.Fatalf("begins=%d commits=%d rollbacks=%d", database.begins, database.commits, database.rollbacks)
			}
			if _, landed := database.committed[key]; landed != test.committed || len(database.held) != 0 {
				t.Fatalf("committed=%v held=%v", database.committed, database.held)
			}
			database.beginErr, database.commitErr, database.rollbackErr = nil, nil, nil
			reuse := edax.WithinTransaction(context.Background(), database, func(txctx context.Context) error {
				return database.claim(txctx, key, record)
			})
			if test.committed != errors.Is(reuse, edax.ErrConcurrency) || (!test.committed && reuse != nil) {
				t.Fatalf("reusing the key: %v", reuse)
			}
		})
	}
}
