// Package edaxtest checks backend implementations against the Go SDK contracts.
// Factories must return an empty, isolated backend and register cleanup with t.
package edaxtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Factory returns an empty, isolated backend and registers resource cleanup with t.
type Factory[T any] func(testing.TB) T

// EventBackend keeps the event log and its transactional outbox together.
type EventBackend interface {
	edax.EventStore
	edax.Outbox[edax.EventMessage]
}

// clause is one named obligation of a contract. It takes testing.TB, so a negative control can run it
// against a deliberately broken backend and read its verdict.
type clause[T any] struct {
	name string
	run  func(t testing.TB, factory Factory[T])
}

func runClauses[T any](t *testing.T, factory Factory[T], clauses []clause[T]) {
	t.Helper()
	for _, obligation := range clauses {
		t.Run(obligation.name, func(t *testing.T) { obligation.run(t, factory) })
	}
}

func ctx(t testing.TB) context.Context {
	t.Helper()
	deadlineCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return deadlineCtx
}
func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// invoiceEvent drafts an event on the stream of invoice number.
func invoiceEvent(number, id, name string, sequence uint64) edax.EventDraft {
	data := fmt.Sprintf(`{"invoice_id":%q,"total":"49.90","currency":"EUR"}`, number)
	return edax.EventDraft{ID: id, AggregateID: "invoice:" + number, Sequence: &sequence, Name: name, Version: 1, Data: []byte(data), CorrelationID: "checkout:" + number, CausationID: "issue:" + number}
}
func draft(id string, sequence uint64) edax.EventDraft {
	return invoiceEvent("1042", id, "InvoiceIssued", sequence)
}

// invoiceCommand builds the command id asks of invoice number.
func invoiceCommand(number, id, name string) edax.CommandMessage {
	data := fmt.Sprintf(`{"invoice_id":%q,"total":"49.90","currency":"EUR"}`, number)
	return edax.CommandMessage{ID: id, Name: name, Version: 1, Data: []byte(data), CorrelationID: "checkout:" + number, CausationID: "issued:" + number, IssuedAt: 1760000000000}
}
func command() edax.CommandMessage { return invoiceCommand("1042", "charge:1042", "ChargeInvoice") }

func sagaCommand(t testing.TB) edax.CommandMessage {
	t.Helper()
	return sagaStepCommand(t, 1, "ChargeInvoice")
}

// sagaStepCommand is the command the billing saga's step at version issues, named name.
func sagaStepCommand(t testing.TB, version uint64, name string) edax.CommandMessage {
	t.Helper()
	payload := command()
	result, err := kernel.SagaCommand("billing", "invoice:1042", version, 0, edax.Payload{Name: name, Version: payload.Version, Data: payload.Data}, payload.CausationID, payload.CorrelationID, payload.IssuedAt, nil, nil)
	check(t, err)
	return *result
}

func ids[M edax.EventMessage | edax.CommandMessage](messages []M) []string {
	out := make([]string, len(messages))
	for index, message := range messages {
		switch message := any(message).(type) {
		case edax.EventMessage:
			out[index] = message.ID
		case edax.CommandMessage:
			out[index] = message.ID
		}
	}
	return out
}

// EventStore checks optimistic concurrency, batch atomicity, snapshots, competing writers and outbox order.
func EventStore(t *testing.T, factory Factory[EventBackend]) {
	t.Helper()
	runClauses(t, factory, eventStoreClauses())
}

func eventStoreClauses() []clause[EventBackend] {
	return []clause[EventBackend]{
		{"append_and_outbox", appendAndOutbox},
		{"stale_version", staleVersion},
		{"duplicate_batch", duplicateBatch},
		{"snapshots", snapshots},
		{"two_writers_at_one_version_land_exactly_one_append", twoWritersLandOneAppend},
		{"unpublished_is_oldest_commit_first", eventOutboxOldestCommitFirst},
	}
}

// appendIssued stores the first event of invoice 1042, the state every single-writer clause starts from.
func appendIssued(t testing.TB, store EventBackend, ctx context.Context) {
	t.Helper()
	first, err := store.Append(ctx, "invoice:1042", 0, []edax.EventDraft{draft("issued:1042", 1)})
	check(t, err)
	if len(first) != 1 || first[0].Sequence == nil || *first[0].Sequence != 1 {
		t.Fatalf("stored event: %+v", first)
	}
}

func appendAndOutbox(t testing.TB, factory Factory[EventBackend]) {
	store := factory(t)
	ctx := ctx(t)
	appendIssued(t, store, ctx)
	loaded, err := store.Load(ctx, "invoice:1042", 0)
	check(t, err)
	if len(loaded) != 1 || loaded[0].ID != "issued:1042" || loaded[0].CorrelationID != "checkout:1042" {
		t.Fatalf("lost envelope fields: %+v", loaded)
	}
	after, err := store.Load(ctx, "invoice:1042", 1)
	check(t, err)
	if len(after) != 0 {
		t.Fatalf("after is not exclusive: %+v", after)
	}
	pending, err := store.Unpublished(ctx, 10)
	check(t, err)
	if len(pending) != 1 || pending[0].ID != loaded[0].ID {
		t.Fatalf("outbox: %+v", pending)
	}
	for range 2 {
		check(t, store.MarkPublished(ctx, []string{pending[0].ID}))
	}
	pending, err = store.Unpublished(ctx, 10)
	check(t, err)
	if len(pending) != 0 {
		t.Fatalf("published rows replayed: %+v", pending)
	}
}

func staleVersion(t testing.TB, factory Factory[EventBackend]) {
	store := factory(t)
	ctx := ctx(t)
	appendIssued(t, store, ctx)
	_, err := store.Append(ctx, "invoice:1042", 0, []edax.EventDraft{draft("paid:1042", 2)})
	if !errors.Is(err, edax.ErrConcurrency) {
		t.Fatalf("stale append: %v", err)
	}
	rows, err := store.Load(ctx, "invoice:1042", 0)
	check(t, err)
	if len(rows) != 1 {
		t.Fatalf("failed append wrote rows: %+v", rows)
	}
}

func duplicateBatch(t testing.TB, factory Factory[EventBackend]) {
	store := factory(t)
	ctx := ctx(t)
	appendIssued(t, store, ctx)
	_, err := store.Append(ctx, "invoice:1042", 1, []edax.EventDraft{draft("paid:1042", 2), draft("issued:1042", 3)})
	if !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("duplicate event id: %v", err)
	}
	rows, err := store.Load(ctx, "invoice:1042", 0)
	check(t, err)
	if len(rows) != 1 {
		t.Fatalf("batch partially written: %+v", rows)
	}
	pending, err := store.Unpublished(ctx, 10)
	check(t, err)
	if len(pending) != 1 {
		t.Fatalf("failed batch reached outbox: %+v", pending)
	}
}

func snapshots(t testing.TB, factory Factory[EventBackend]) {
	store := factory(t)
	ctx := ctx(t)
	appendIssued(t, store, ctx)
	snapshot := edax.Snapshot{AggregateID: "invoice:1042", TypeName: "Invoice", Version: 1, Data: []byte(`{"paid":false,"total":"49.90"}`)}
	check(t, store.SaveSnapshot(ctx, snapshot))
	loaded, err := store.LoadSnapshot(ctx, "invoice:1042")
	check(t, err)
	if loaded == nil || loaded.Version != 1 || string(loaded.Data) != string(snapshot.Data) {
		t.Fatalf("snapshot: %+v", loaded)
	}
	snapshot.Version = 2
	if err = store.SaveSnapshot(ctx, snapshot); !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("snapshot ahead of stream: %v", err)
	}
}

// twoWritersLandOneAppend: two units of work both append at version 0, each staging before either
// commits. A store that checks the version and then inserts lets both land, forking the stream.
func twoWritersLandOneAppend(t testing.TB, factory Factory[EventBackend]) {
	store := factory(t)
	issue, void := draft("issued:1042", 1), invoiceEvent("1042", "voided:1042", "InvoiceVoided", 1)
	appendOne := func(event edax.EventDraft) func(context.Context) error {
		return func(txctx context.Context) error {
			_, err := store.Append(txctx, "invoice:1042", 0, []edax.EventDraft{event})
			return err
		}
	}
	winner := landedOnce(t, race(t, store.UnitOfWork(), appendOne(issue), appendOne(void)), issue.ID, void.ID)
	rows, err := store.Load(ctx(t), "invoice:1042", 0)
	check(t, err)
	if got := ids(rows); !slices.Equal(got, []string{winner}) {
		t.Fatalf("the stream holds %v, want the winner %s alone", got, winner)
	}
}

// eventOutboxOldestCommitFirst: a relay publishes in the order unpublished reads, so a store that pages
// newest first sends a later decision ahead of an earlier one. The limit cuts the oldest page.
func eventOutboxOldestCommitFirst(t testing.TB, factory Factory[EventBackend]) {
	store := factory(t)
	ctx := ctx(t)
	commits := []struct {
		stream   string
		expected uint64
		drafts   []edax.EventDraft
	}{
		{"invoice:1042", 0, []edax.EventDraft{draft("issued:1042", 1), invoiceEvent("1042", "sent:1042", "InvoiceSent", 2)}},
		{"invoice:1043", 0, []edax.EventDraft{invoiceEvent("1043", "issued:1043", "InvoiceIssued", 1)}},
		{"invoice:1042", 2, []edax.EventDraft{invoiceEvent("1042", "paid:1042", "InvoicePaid", 3), invoiceEvent("1042", "receipted:1042", "ReceiptSent", 4)}},
	}
	var want []string
	for _, batch := range commits {
		check(t, edax.WithinTransaction(ctx, store.UnitOfWork(), func(txctx context.Context) error {
			_, err := store.Append(txctx, batch.stream, batch.expected, batch.drafts)
			return err
		}))
		for _, event := range batch.drafts {
			want = append(want, event.ID)
		}
	}
	rows, err := store.Unpublished(ctx, 10)
	check(t, err)
	if got := ids(rows); !slices.Equal(got, want) {
		t.Fatalf("outbox order %v, want oldest commit first %v", got, want)
	}
	page, err := store.Unpublished(ctx, 2)
	check(t, err)
	if got := ids(page); !slices.Equal(got, want[:2]) {
		t.Fatalf("outbox order of a limited page %v, want %v", got, want[:2])
	}
}

// IdempotencyStore checks exclusive claims, concurrent claimants, stale handles and settled replay.
func IdempotencyStore(t *testing.T, factory Factory[edax.IdempotencyStore]) {
	t.Helper()
	runClauses(t, factory, idempotencyStoreClauses())
}

func idempotencyStoreClauses() []clause[edax.IdempotencyStore] {
	clauses := []clause[edax.IdempotencyStore]{}
	for _, name := range []string{"claim_and_replay", "stale_handle", "forget"} {
		clauses = append(clauses, clause[edax.IdempotencyStore]{name, func(t testing.TB, factory Factory[edax.IdempotencyStore]) { claimLifecycle(t, factory, name) }})
	}
	return append(clauses,
		clause[edax.IdempotencyStore]{"concurrent_claims_have_exactly_one_winner", concurrentClaimsHaveOneWinner},
		clause[edax.IdempotencyStore]{"a_cached_rejection_keeps_its_code_and_reason", cachedRejectionKeepsItsReason},
	)
}

// cachedRejectionKeepsItsReason: a settled rejection replays with its code and the reason it named. A
// ledger that rebuilds the record from the fields it knows loses them, and a replay then cannot tell
// not_found from another rejection.
func cachedRejectionKeepsItsReason(t testing.TB, factory Factory[edax.IdempotencyStore]) {
	store := factory(t)
	ctx := ctx(t)
	key := "billing:charge:1042"
	claimed, err := store.Claim(ctx, key, 42)
	check(t, err)
	message, code, reason := "no invoice 1042", "rejected", "not_found"
	record := edax.IdempotencyRecord{PayloadHash: 42, State: "failed", Error: &message, ErrorCode: &code, ErrorReason: &reason}
	check(t, store.Complete(ctx, key, claimed.Handle, record))
	prior, err := store.Claim(ctx, key, 42)
	check(t, err)
	if prior.Won || prior.Prior == nil {
		t.Fatal("a settled rejection was not replayed")
	}
	decoded, err := kernel.DecodeIdempotencyRecord(*prior.Prior)
	check(t, err)
	if decoded.State != "failed" || decoded.ErrorCode == nil || *decoded.ErrorCode != code || decoded.ErrorReason == nil || *decoded.ErrorReason != reason {
		t.Fatalf("cached rejection reads %+v, want code %s and reason %s", decoded, code, reason)
	}
}

func claimLifecycle(t testing.TB, factory Factory[edax.IdempotencyStore], name string) {
	store := factory(t)
	ctx := ctx(t)
	key := "billing:charge:1042"
	first, err := store.Claim(ctx, key, 42)
	check(t, err)
	if !first.Won {
		t.Fatal("empty key was not claimed")
	}
	other, err := store.Claim(ctx, key, 42)
	check(t, err)
	if other.Won || other.Prior == nil {
		t.Fatalf("second claimant won: %+v", other)
	}
	result := `{"invoice_id":"1042","paid":true}`
	record := edax.IdempotencyRecord{PayloadHash: 42, State: "done", ResultJSON: &result}
	if name == "stale_handle" {
		check(t, store.Release(ctx, key, first.Handle))
		next, err := store.Claim(ctx, key, 42)
		check(t, err)
		if !next.Won {
			t.Fatal("released claim still held")
		}
		if err = store.Complete(ctx, key, first.Handle, record); err == nil {
			t.Fatal("stale complete succeeded")
		}
		check(t, store.Release(ctx, key, first.Handle))
		check(t, store.Complete(ctx, key, next.Handle, record))
		return
	}
	if name == "forget" {
		if err = store.Forget(ctx, key); !errors.Is(err, edax.ErrInProgress) {
			t.Fatalf("forget active claim: %v", err)
		}
	}
	check(t, store.Complete(ctx, key, first.Handle, record))
	check(t, store.Release(ctx, key, first.Handle))
	prior, err := store.Claim(ctx, key, 42)
	check(t, err)
	if prior.Won || prior.Prior == nil {
		t.Fatal("settled release removed cached record")
	}
	decoded, err := kernel.DecodeIdempotencyRecord(*prior.Prior)
	check(t, err)
	if decoded.State != "done" || decoded.PayloadHash != 42 || decoded.ResultJSON == nil || *decoded.ResultJSON != result {
		t.Fatalf("cached result: %+v", decoded)
	}
	if name == "forget" {
		check(t, store.Forget(ctx, key))
		next, err := store.Claim(ctx, key, 42)
		check(t, err)
		if !next.Won {
			t.Fatal("forgotten key did not admit a new claim")
		}
	}
}

// concurrentClaimsHaveOneWinner: eight claims on one key at once, the shape of a redelivery storm. A
// store that reads, then writes in a second call, lets two of them read "free" and run the handler twice.
func concurrentClaimsHaveOneWinner(t testing.TB, factory Factory[edax.IdempotencyStore]) {
	store := factory(t)
	ctx := ctx(t)
	const claimants = 8
	results := make([]*edax.ClaimResult, claimants)
	failures := make([]error, claimants)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range claimants {
		group.Go(func() {
			<-start
			results[index], failures[index] = store.Claim(ctx, "billing:charge:1042", 42)
		})
	}
	close(start)
	group.Wait()
	won := 0
	for index, result := range results {
		check(t, failures[index])
		if result.Won {
			won++
			continue
		}
		if result.Prior == nil {
			t.Fatalf("a losing claimant read no record: %+v", result)
		}
		decoded, err := kernel.DecodeIdempotencyRecord(*result.Prior)
		check(t, err)
		if decoded.State != "pending" || decoded.PayloadHash != 42 {
			t.Fatalf("a losing claimant read %+v, want the winner's pending claim", decoded)
		}
	}
	if won != 1 {
		t.Fatalf("%d of %d concurrent claimants won, want exactly one winner", won, claimants)
	}
}

// ProjectionStore checks keyed rows, optimistic versions, refused documents, competing writers and the
// kernel's paging through GetMany.
func ProjectionStore(t *testing.T, factory Factory[edax.ProjectionStore]) {
	t.Helper()
	runClauses(t, factory, projectionStoreClauses())
}

// NamedProjectionStores checks that read models over one database share no row. The factory returns a
// function that opens the store of a read-model name, every one over the same database. Call it beside
// ProjectionStore for a backend that keeps more than one read model.
func NamedProjectionStores(t *testing.T, factory Factory[func(name string) edax.ProjectionStore]) {
	t.Helper()
	runClauses(t, factory, namedProjectionStoreClauses())
}

func namedProjectionStoreClauses() []clause[func(string) edax.ProjectionStore] {
	return []clause[func(string) edax.ProjectionStore]{{"two_read_models_share_no_rows", readModelsShareNoRows}}
}

// readModelsShareNoRows: an id one read model saved is absent from another, whose first save at version 0
// makes its own row, and each pages only its own. Without it a second read model keyed by the same ids
// overwrites the first. The names and ids would collide if a store joined them with a separator.
func readModelsShareNoRows(t testing.TB, factory Factory[func(string) edax.ProjectionStore]) {
	named := factory(t)
	first, second := named("invoices"), named("invoices:q")
	ctx := ctx(t)
	_, err := first.Save(ctx, "q:1042", 0, `{"from":"first"}`)
	check(t, err)
	for _, id := range []string{"q:1042", "1042"} {
		row, err := second.Get(ctx, id)
		check(t, err)
		if row != nil {
			t.Fatalf("the other read model's id %s is present: %+v", id, row)
		}
	}
	version, err := second.Save(ctx, "q:1042", 0, `{"from":"second"}`)
	check(t, err)
	if version != 1 {
		t.Fatalf("a first save in the second read model reached version %d, want 1", version)
	}
	wantRow(t, first, ctx, "q:1042", 1, map[string]any{"id": "q:1042", "from": "first"})
	for _, read := range []struct {
		store edax.ProjectionStore
		from  string
	}{{first, "first"}, {second, "second"}} {
		page, err := read.store.GetMany(ctx, `{}`)
		check(t, err)
		var parsed struct {
			Rows []map[string]any `json:"rows"`
		}
		check(t, json.Unmarshal([]byte(page), &parsed))
		want := []map[string]any{{"id": "q:1042", "from": read.from}}
		if !reflect.DeepEqual(parsed.Rows, want) {
			t.Fatalf("%s read model pages %v, want %v", read.from, parsed.Rows, want)
		}
	}
}

// refusedSaves are saves over a row stored at version 1, by branch: the version the writer read, the
// document it saves, and the refusal. Each writes nothing.
var refusedSaves = []struct {
	name     string
	version  uint64
	document string
	refusal  error
}{
	{"stale", 0, `{"invoice_id":"1042","paid":true,"total":"49.90"}`, edax.ErrConcurrency},
	{"ahead", 7, `{"invoice_id":"1042","paid":true,"total":"49.90"}`, edax.ErrConcurrency},
	// Not documents at all, as an untyped caller can still hand them over.
	{"a_list", 1, `["billing@example.com"]`, edax.ErrCodec},
	{"a_scalar", 1, `36`, edax.ErrCodec},
	{"null", 1, `null`, edax.ErrCodec},
}

func projectionStoreClauses() []clause[edax.ProjectionStore] {
	clauses := []clause[edax.ProjectionStore]{{"keyed_row_and_version", keyedRowAndVersion}}
	for _, branch := range refusedSaves {
		clauses = append(clauses, clause[edax.ProjectionStore]{"refused_save/" + branch.name, func(t testing.TB, factory Factory[edax.ProjectionStore]) {
			store := factory(t)
			ctx := ctx(t)
			_, err := store.Save(ctx, "invoice:1042", 0, `{"invoice_id":"1042","paid":false,"total":"49.90"}`)
			check(t, err)
			if _, err = store.Save(ctx, "invoice:1042", branch.version, branch.document); !errors.Is(err, branch.refusal) {
				t.Fatalf("%s save was not refused with %v: %v", branch.name, branch.refusal, err)
			}
			wantRow(t, store, ctx, "invoice:1042", 1, map[string]any{"id": "invoice:1042", "invoice_id": "1042", "paid": false, "total": "49.90"})
		}})
	}
	return append(clauses,
		clause[edax.ProjectionStore]{"two_writers_at_one_version_land_exactly_one_save", twoWritersLandOneSave},
		clause[edax.ProjectionStore]{"get_many_returns_the_row_key_as_the_id", getManyKeysRows},
		clause[edax.ProjectionStore]{"get_many_pages_every_row_once_by_its_next_cursor", getManyPagesOnce},
	)
}

// wantRow fails unless id is stored at version with exactly document.
func wantRow(t testing.TB, store edax.ProjectionStore, ctx context.Context, id string, version uint64, document map[string]any) {
	t.Helper()
	row, err := store.Get(ctx, id)
	check(t, err)
	if row == nil || row.Version != version {
		t.Fatalf("projection %s: %+v, want version %d", id, row, version)
	}
	var parsed map[string]any
	check(t, json.Unmarshal([]byte(row.DocumentJSON), &parsed))
	if !reflect.DeepEqual(parsed, document) {
		t.Fatalf("projection %s reads %v, want %v", id, parsed, document)
	}
}

// keyedRowAndVersion: Save returns the new version, and Get reads the document keyed by its row, so an
// "id" the saved document carried is overwritten. A store that reads the document alone disagrees with
// GetMany on what a row is.
func keyedRowAndVersion(t testing.TB, factory Factory[edax.ProjectionStore]) {
	store := factory(t)
	ctx := ctx(t)
	absent, err := store.Get(ctx, "invoice:1042")
	check(t, err)
	if absent != nil {
		t.Fatal("missing projection is not nil")
	}
	version, err := store.Save(ctx, "invoice:1042", 0, `{"id":"stale","invoice_id":"1042","paid":false,"total":"49.90"}`)
	check(t, err)
	if version != 1 {
		t.Fatalf("initial version %d", version)
	}
	wantRow(t, store, ctx, "invoice:1042", 1, map[string]any{"id": "invoice:1042", "invoice_id": "1042", "paid": false, "total": "49.90"})
}

// twoWritersLandOneSave: two projectors both save a new row at version 0, each staging before either
// commits. A store that reads the version and then writes lets the second overwrite the first.
func twoWritersLandOneSave(t testing.TB, factory Factory[edax.ProjectionStore]) {
	store := factory(t)
	saveStatus := func(status string) func(context.Context) error {
		return func(txctx context.Context) error {
			_, err := store.Save(txctx, "invoice:1042", 0, `{"invoice_id":"1042","status":"`+status+`"}`)
			return err
		}
	}
	winner := landedOnce(t, race(t, store.UnitOfWork(), saveStatus("paid"), saveStatus("voided")), "paid", "voided")
	wantRow(t, store, ctx(t), "invoice:1042", 1, map[string]any{"id": "invoice:1042", "invoice_id": "1042", "status": winner})
}

// getManyKeysRows: each GetMany row carries its row key as "id", overwriting one the document held. The
// kernel sorts its keyset tiebreak on "id" and mints every cursor from it.
func getManyKeysRows(t testing.TB, factory Factory[edax.ProjectionStore]) {
	store := factory(t)
	ctx := ctx(t)
	_, err := store.Save(ctx, "invoice:1042", 0, `{"customer_id":"customer-17","total":"49.90"}`)
	check(t, err)
	_, err = store.Save(ctx, "invoice:1043", 0, `{"id":"stale","customer_id":"customer-17","total":"12.00"}`)
	check(t, err)
	page, err := edax.QueryEntities(ctx, store, edax.QuerySpec{Filter: edax.Equal("customer_id", "customer-17"), Sort: []edax.SortKey{{Path: "total"}}})
	check(t, err)
	keys := make([]string, len(page.Rows))
	for index, row := range page.Rows {
		keys[index], _ = row["id"].(string)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"invoice:1042", "invoice:1043"}) {
		t.Fatalf("GetMany ids %v, want each row key", keys)
	}
}

// getManyPagesOnce: keyset paging resumes strictly after the cursor, and mints a next cursor only while
// rows follow. Rows that tie on the sort key are told apart by id, and a row committed after the cursor
// arrives, so a reader following next sees each row once and stops on the last page.
func getManyPagesOnce(t testing.TB, factory Factory[edax.ProjectionStore]) {
	store := factory(t)
	ctx := ctx(t)
	dueDays := []int{2, 1, 1, 1, 3}
	for index, due := range dueDays {
		_, err := store.Save(ctx, fmt.Sprintf("invoice:10%02d", index), 0, fmt.Sprintf(`{"customer_id":"customer-17","due_day":%d}`, due))
		check(t, err)
	}
	// The kernel's order: due day, then id.
	order := []string{"invoice:1001", "invoice:1002", "invoice:1003", "invoice:1000", "invoice:1004"}
	spec := edax.QuerySpec{Filter: edax.Equal("customer_id", "customer-17"), Sort: []edax.SortKey{{Path: "due_day"}}, Limit: edax.Ptr(uint64(2))}
	page, err := edax.QueryEntities(ctx, store, spec)
	check(t, err)
	var seen []string
	collect := func(rows []edax.Document) {
		for _, row := range rows {
			id, _ := row["id"].(string)
			seen = append(seen, id)
		}
	}
	collect(page.Rows)
	_, err = store.Save(ctx, "invoice:1099", 0, `{"customer_id":"customer-17","due_day":9}`) // after the cursor
	check(t, err)
	_, err = store.Save(ctx, "invoice:1098", 0, `{"customer_id":"customer-17","due_day":0}`) // before it
	check(t, err)
	// Bounded, so a store whose cursor repeats a row fails rather than pages forever.
	for range len(order) + 2 {
		if page.Next == nil {
			break
		}
		spec.After = page.Next
		page, err = edax.QueryEntities(ctx, store, spec)
		check(t, err)
		collect(page.Rows)
	}
	if want := append(order, "invoice:1099"); !slices.Equal(seen, want) {
		t.Fatalf("paged %v, want every row once by its next cursor %v", seen, want)
	}
}

// CommandOutbox checks enqueue, ordered reads over many rows and idempotent publication marks.
func CommandOutbox(t *testing.T, factory Factory[edax.CommandOutbox]) {
	t.Helper()
	runClauses(t, factory, commandOutboxClauses())
}

func commandOutboxClauses() []clause[edax.CommandOutbox] {
	return []clause[edax.CommandOutbox]{
		{"enqueue_and_mark_published", enqueueAndMark},
		{"unpublished_is_oldest_commit_first", commandOutboxOldestCommitFirst},
	}
}

func enqueueAndMark(t testing.TB, factory Factory[edax.CommandOutbox]) {
	store := factory(t)
	ctx := ctx(t)
	message := command()
	check(t, edax.WithinTransaction(ctx, store.UnitOfWork(), func(ctx context.Context) error { return store.Enqueue(ctx, []edax.CommandMessage{message}, "") }))
	rows, err := store.Unpublished(ctx, 10)
	check(t, err)
	if len(rows) != 1 || rows[0].ID != message.ID {
		t.Fatalf("outbox: %+v", rows)
	}
	for range 2 {
		check(t, store.MarkPublished(ctx, []string{message.ID}))
	}
	rows, err = store.Unpublished(ctx, 10)
	check(t, err)
	if len(rows) != 0 {
		t.Fatalf("published commands still pending: %+v", rows)
	}
}

// commandOutboxOldestCommitFirst: the relay sends in the order unpublished reads, so a store that pages
// newest first sends a later decision about an invoice ahead of an earlier one.
func commandOutboxOldestCommitFirst(t testing.TB, factory Factory[edax.CommandOutbox]) {
	store := factory(t)
	ctx := ctx(t)
	commits := [][]edax.CommandMessage{
		{command(), invoiceCommand("1042", "notify:1042", "NotifyCustomer")},
		{invoiceCommand("1043", "charge:1043", "ChargeInvoice")},
		{invoiceCommand("1042", "refund:1042", "RefundInvoice"), invoiceCommand("1043", "notify:1043", "NotifyCustomer")},
	}
	var want []string
	for _, batch := range commits {
		check(t, edax.WithinTransaction(ctx, store.UnitOfWork(), func(txctx context.Context) error { return store.Enqueue(txctx, batch, "") }))
		want = append(want, ids(batch)...)
	}
	rows, err := store.Unpublished(ctx, 10)
	check(t, err)
	if got := ids(rows); !slices.Equal(got, want) {
		t.Fatalf("outbox order %v, want oldest commit first %v", got, want)
	}
	page, err := store.Unpublished(ctx, 2)
	check(t, err)
	if got := ids(page); !slices.Equal(got, want[:2]) {
		t.Fatalf("outbox order of a limited page %v, want %v", got, want[:2])
	}
}

// SagaStore checks version conflicts, state and folded marks, commands, competing steps and tombstones.
func SagaStore(t *testing.T, factory Factory[edax.SagaStore]) {
	t.Helper()
	runClauses(t, factory, sagaStoreClauses())
}

func sagaStoreClauses() []clause[edax.SagaStore] {
	return []clause[edax.SagaStore]{
		{"save_load_and_complete", sagaLifecycle},
		{"two_steps_at_one_version_land_exactly_one_write", twoStepsLandOneWrite},
	}
}

// sagaFolded is the folded-event marks the kernel kept after one sequenced event of invoice 1042.
const sagaFolded = `{"invoice:1042":1}`

func sagaLifecycle(t testing.TB, factory Factory[edax.SagaStore]) {
	store := factory(t)
	ctx := ctx(t)
	missing, err := store.Load(ctx, "billing", "invoice:1042")
	check(t, err)
	if missing != nil {
		t.Fatal("missing saga is not nil")
	}
	version, err := store.Save(ctx, "billing", "invoice:1042", json.RawMessage(`{"phase":"charging"}`), 0, sagaFolded, []edax.CommandMessage{sagaCommand(t)})
	check(t, err)
	if version != 1 {
		t.Fatalf("saga version %d", version)
	}
	_, err = store.Save(ctx, "billing", "invoice:1042", json.RawMessage(`{"phase":"wrong"}`), 0, "{}", nil)
	if !errors.Is(err, edax.ErrConcurrency) {
		t.Fatalf("stale saga write: %v", err)
	}
	instance, err := store.Load(ctx, "billing", "invoice:1042")
	check(t, err)
	if instance == nil || instance.Version != 1 || instance.Completed {
		t.Fatalf("saga instance: %+v", instance)
	}
	if string(instance.State) != `{"phase":"charging"}` || instance.Folded != sagaFolded {
		t.Fatalf("saga state %s and folded marks %q, want what the step saved", instance.State, instance.Folded)
	}
	commands, err := store.Unpublished(ctx, "billing", 10)
	check(t, err)
	if len(commands) != 1 || commands[0].ID != sagaCommand(t).ID {
		t.Fatalf("saga outbox: %+v", commands)
	}
	_, err = store.Complete(ctx, "billing", "invoice:1042", version, nil)
	check(t, err)
	instance, err = store.Load(ctx, "billing", "invoice:1042")
	check(t, err)
	if instance == nil || !instance.Completed {
		t.Fatalf("missing completion tombstone: %+v", instance)
	}
	for range 2 {
		check(t, store.MarkPublished(ctx, "billing", []string{sagaCommand(t).ID}))
	}
	commands, err = store.Unpublished(ctx, "billing", 10)
	check(t, err)
	if len(commands) != 0 {
		t.Fatal("published saga command remained")
	}
}

// twoStepsLandOneWrite: two workers fold the same instance at version 0, each staging state and a
// command before either commits. A store that reads the version and then writes lets both land: the
// later state wins, and both commands go out.
func twoStepsLandOneWrite(t testing.TB, factory Factory[edax.SagaStore]) {
	store := factory(t)
	step := func(phase, name string) func(context.Context) error {
		return func(txctx context.Context) error {
			_, err := store.Save(txctx, "billing", "invoice:1042", json.RawMessage(`{"phase":"`+phase+`"}`), 0, sagaFolded, []edax.CommandMessage{sagaStepCommand(t, 1, name)})
			return err
		}
	}
	winner := landedOnce(t, race(t, store.UnitOfWork(), step("charging", "ChargeInvoice"), step("voiding", "VoidInvoice")), "charging", "voiding")
	ctx := ctx(t)
	instance, err := store.Load(ctx, "billing", "invoice:1042")
	check(t, err)
	if instance == nil || instance.Version != 1 || string(instance.State) != `{"phase":"`+winner+`"}` {
		t.Fatalf("saga instance %+v, want the winner's %s state", instance, winner)
	}
	commands, err := store.Unpublished(ctx, "billing", 10)
	check(t, err)
	want := map[string]string{"charging": "ChargeInvoice", "voiding": "VoidInvoice"}[winner]
	if len(commands) != 1 || commands[0].Name != want {
		t.Fatalf("saga outbox %+v, want the winner's %s alone", commands, want)
	}
}

// TransactionFixture's stores must share Owner and the same physical transaction.
type TransactionFixture struct {
	Owner       edax.UnitOfWork
	Events      EventBackend
	Ledger      edax.IdempotencyStore
	Commands    edax.CommandOutbox
	Projections edax.ProjectionStore
	Sagas       edax.SagaStore
}

// participants is what a reader sees of every participant of a TransactionFixture.
type participants struct {
	events, eventOutbox, commandOutbox, sagaCommands int
	snapshot, projection, saga, claimTaken           bool
}

func visible(t testing.TB, fixture TransactionFixture, ctx context.Context) participants {
	t.Helper()
	var seen participants
	rows, err := fixture.Events.Load(ctx, "invoice:1042", 0)
	check(t, err)
	seen.events = len(rows)
	outbox, err := fixture.Events.Unpublished(ctx, 10)
	check(t, err)
	seen.eventOutbox = len(outbox)
	snapshot, err := fixture.Events.LoadSnapshot(ctx, "invoice:1042")
	check(t, err)
	seen.snapshot = snapshot != nil
	commands, err := fixture.Commands.Unpublished(ctx, 10)
	check(t, err)
	seen.commandOutbox = len(commands)
	projection, err := fixture.Projections.Get(ctx, "invoice:1042")
	check(t, err)
	seen.projection = projection != nil
	saga, err := fixture.Sagas.Load(ctx, "billing", "invoice:1042")
	check(t, err)
	seen.saga = saga != nil
	sagaCommands, err := fixture.Sagas.Unpublished(ctx, "billing", 10)
	check(t, err)
	seen.sagaCommands = len(sagaCommands)
	return seen
}

// Probe the ledger only after settlement: a valid backend may block on an uncommitted claim.
// A winning probe rolls back so it never takes the key itself.
func claimTaken(t testing.TB, fixture TransactionFixture, ctx context.Context) bool {
	t.Helper()
	var taken bool
	undo := errors.New("undo the claim probe")
	probe := edax.WithinTransaction(ctx, fixture.Owner, func(probeCtx context.Context) error {
		claim, err := fixture.Ledger.Claim(probeCtx, "billing:charge:1042", 42)
		if err != nil {
			return err
		}
		taken = !claim.Won
		if !claim.Won && claim.Prior == nil {
			t.Fatal("committed ledger record is missing")
		}
		if claim.Prior != nil {
			record, err := kernel.DecodeIdempotencyRecord(*claim.Prior)
			if err != nil {
				return err
			}
			if record.State != "done" || record.PayloadHash != 42 || record.ResultJSON == nil || *record.ResultJSON != `{"paid":true}` {
				t.Fatalf("committed ledger record: %+v", record)
			}
		}
		return undo
	})
	if !errors.Is(probe, undo) {
		t.Fatalf("claim probe: %v", probe)
	}
	return taken
}

// Transactions verifies atomic commit and rollback across all stores in each fresh fixture: events,
// snapshot, ledger, both outboxes, projection and saga state.
func Transactions(t *testing.T, factory Factory[TransactionFixture]) {
	t.Helper()
	runClauses(t, factory, transactionClauses())
}

func transactionClauses() []clause[TransactionFixture] {
	return []clause[TransactionFixture]{
		{"commit", func(t testing.TB, factory Factory[TransactionFixture]) { settleTogether(t, factory, false) }},
		{"rollback", func(t testing.TB, factory Factory[TransactionFixture]) { settleTogether(t, factory, true) }},
		{"commands_wait_behind_their_event", commandsWaitBehindTheirEvent},
	}
}

// commandsWaitBehindTheirEvent: a command waits behind the event its save appended, and every later command
// of the same aggregate and tenant waits behind it, so the relay sends them in order and never before the
// event. Another aggregate's command waits for nothing.
func commandsWaitBehindTheirEvent(t testing.TB, factory Factory[TransactionFixture]) {
	fixture := factory(t)
	ctx := ctx(t)
	event := draft("issued:1042", 1)
	keyed := func(command edax.CommandMessage, aggregate string) edax.CommandMessage {
		command.AggregateID = &aggregate
		return command
	}
	first := keyed(invoiceCommand("1042", "charge:1042", "ChargeInvoice"), "invoice:1042")
	second := keyed(invoiceCommand("1042", "notify:1042", "NotifyCustomer"), "invoice:1042")
	elsewhere := keyed(invoiceCommand("1043", "charge:1043", "ChargeInvoice"), "invoice:1043")
	check(t, edax.WithinTransaction(ctx, fixture.Owner, func(txctx context.Context) error {
		if _, err := fixture.Events.Append(txctx, "invoice:1042", 0, []edax.EventDraft{event}); err != nil {
			return err
		}
		return fixture.Commands.Enqueue(txctx, []edax.CommandMessage{first}, event.ID)
	}))
	check(t, edax.WithinTransaction(ctx, fixture.Owner, func(txctx context.Context) error {
		return fixture.Commands.Enqueue(txctx, []edax.CommandMessage{second, elsewhere}, "")
	}))
	rows, err := fixture.Commands.Unpublished(ctx, 10)
	check(t, err)
	if got := ids(rows); !slices.Equal(got, ids([]edax.CommandMessage{elsewhere})) {
		t.Fatalf("waiting commands %v, want only another aggregate's", got)
	}
	check(t, fixture.Events.MarkPublished(ctx, []string{event.ID}))
	rows, err = fixture.Commands.Unpublished(ctx, 10)
	check(t, err)
	if got := ids(rows); !slices.Equal(got, ids([]edax.CommandMessage{first, second, elsewhere})) {
		t.Fatalf("released commands %v, want them in order", got)
	}
}

func settleTogether(t testing.TB, factory Factory[TransactionFixture], rollback bool) {
	fixture := factory(t)
	ctx := ctx(t)
	failure := errors.New("billing operation refused")
	nothing := participants{}
	err := edax.WithinTransaction(ctx, fixture.Owner, func(txctx context.Context) error {
		if _, err := fixture.Events.Append(txctx, "invoice:1042", 0, []edax.EventDraft{draft("issued:1042", 1)}); err != nil {
			return err
		}
		rows, err := fixture.Events.Load(txctx, "invoice:1042", 0)
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			t.Fatal("transaction cannot read its writes")
		}
		if err = fixture.Events.SaveSnapshot(txctx, edax.Snapshot{AggregateID: "invoice:1042", TypeName: "Invoice", Version: 1, Data: []byte(`{"paid":false,"total":"49.90"}`)}); err != nil {
			return err
		}
		claim, err := fixture.Ledger.Claim(txctx, "billing:charge:1042", 42)
		if err != nil {
			return err
		}
		if !claim.Won {
			t.Fatal("transaction lost empty claim")
		}
		result := `{"paid":true}`
		if err = fixture.Ledger.Complete(txctx, "billing:charge:1042", claim.Handle, edax.IdempotencyRecord{PayloadHash: 42, State: "done", ResultJSON: &result}); err != nil {
			return err
		}
		if err = fixture.Commands.Enqueue(txctx, []edax.CommandMessage{command()}, ""); err != nil {
			return err
		}
		if _, err = fixture.Projections.Save(txctx, "invoice:1042", 0, `{"paid":true}`); err != nil {
			return err
		}
		if _, err = fixture.Sagas.Save(txctx, "billing", "invoice:1042", json.RawMessage(`{"phase":"charging"}`), 0, "{}", []edax.CommandMessage{sagaCommand(t)}); err != nil {
			return err
		}
		if outside := visible(t, fixture, edax.WithoutTransaction(txctx)); outside != nothing {
			t.Fatalf("before the commit a reader outside sees %+v, want no staged writes", outside)
		}
		if rollback {
			return failure
		}
		return nil
	})
	want := participants{events: 1, eventOutbox: 1, commandOutbox: 1, sagaCommands: 1, snapshot: true, projection: true, saga: true, claimTaken: true}
	if rollback {
		if !errors.Is(err, failure) {
			t.Fatalf("rollback failure: %v", err)
		}
		want = nothing
	} else {
		check(t, err)
	}
	got := visible(t, fixture, ctx)
	got.claimTaken = claimTaken(t, fixture, ctx)
	if got != want {
		t.Fatalf("after the unit of work every participant shows %+v, want %+v (not atomic)", got, want)
	}
}

// EventBrokerFixture must route these same-aggregate events to one ordered partition.
type EventBrokerFixture struct {
	Publish func(context.Context, edax.EventMessage) error
	Poll    func(context.Context) (*edax.Delivery[edax.EventMessage], error)
}

// EventBroker checks per-key ordering and broker redelivery after negative acknowledgement.
func EventBroker(t *testing.T, factory Factory[EventBrokerFixture]) {
	t.Helper()
	broker := factory(t)
	ctx := ctx(t)
	for i, id := range []string{"issued:1042", "paid:1042"} {
		event, err := kernel.RecordEvent(draft(id, uint64(i+1)), 1760000000000)
		check(t, err)
		check(t, broker.Publish(ctx, *event))
	}
	first, err := broker.Poll(ctx)
	check(t, err)
	second, err := broker.Poll(ctx)
	check(t, err)
	if first == nil || second == nil || first.Message.ID != "issued:1042" || second.Message.ID != "paid:1042" {
		t.Fatal("broker changed per-key order")
	}
	check(t, first.Nack(ctx))
	if second.IsLive() {
		t.Fatal("nack did not retire later delivery")
	}
	replay, err := broker.Poll(ctx)
	check(t, err)
	if replay == nil || replay.Message.ID != first.Message.ID {
		t.Fatal("nack did not replay first message")
	}
	check(t, replay.Ack(ctx))
	check(t, replay.Ack(ctx))
}
