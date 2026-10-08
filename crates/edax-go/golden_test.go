package edax

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

func goldenRows[T any](t *testing.T, name string) []T {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/golden/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []T
	if err = json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}
func goldenError(t *testing.T, got error, want *string) bool {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Fatal(got)
		}
		return false
	}
	if got == nil || got.Error() != *want {
		t.Fatalf("error %v, want %q", got, *want)
	}
	return true
}
func jsonValue(t *testing.T, raw []byte) any {
	t.Helper()
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestHashAndCanonicalGolden(t *testing.T) {
	for index, row := range goldenRows[struct {
		Input    string
		Expected uint64
	}](t, "payload_hash") {
		t.Run(fmt.Sprintf("hash/%d", index), func(t *testing.T) {
			if got := kernel.PayloadHash([]byte(row.Input)); got != row.Expected {
				t.Fatalf("hash %d want %d", got, row.Expected)
			}
		})
	}
	for _, row := range goldenRows[struct {
		Name      string
		Input     string
		Canonical *string
		Hash      *uint64
		Error     *string
	}](t, "canonical_json") {
		t.Run(row.Name, func(t *testing.T) {
			canonical, err := kernel.CanonicalJSON([]byte(row.Input))
			if goldenError(t, err, row.Error) {
				return
			}
			hash, err := PayloadHashJSON([]byte(row.Input))
			if err != nil {
				t.Fatal(err)
			}
			if canonical != *row.Canonical || hash != *row.Hash {
				t.Fatalf("canonical %q hash %d", canonical, hash)
			}
		})
	}
	for _, row := range goldenRows[struct {
		Name     string
		Type     string
		Command  json.RawMessage
		Expected *uint64
		Error    *string
	}](t, "command_hash") {
		t.Run(row.Name, func(t *testing.T) {
			value, err := materializeHost(jsonValue(t, row.Command))
			var hash uint64
			if err == nil {
				raw, encodeErr := encodeJSON(value)
				err = encodeErr
				if err == nil {
					hash, err = kernel.CommandHashJSON(row.Type, raw)
				}
			}
			if goldenError(t, err, row.Error) {
				return
			}
			if hash != *row.Expected {
				t.Fatalf("hash %d want %d", hash, *row.Expected)
			}
		})
	}
}

func TestFilterQueryGolden(t *testing.T) {
	for _, row := range goldenRows[struct {
		Name     string
		Document json.RawMessage
		Filter   json.RawMessage
		Expected *bool
		Error    *string
	}](t, "filter") {
		t.Run("filter/"+row.Name, func(t *testing.T) {
			got, err := kernel.EvalFilter(compactJSON(t, row.Filter), compactJSON(t, row.Document))
			if goldenError(t, err, row.Error) {
				return
			}
			if got != *row.Expected {
				t.Fatalf("got %v want %v", got, *row.Expected)
			}
		})
	}
	for _, row := range goldenRows[struct {
		Name      string
		Documents json.RawMessage
		Query     json.RawMessage
		Expected  json.RawMessage `json:"expected_page"`
		Error     *string
	}](t, "query") {
		t.Run("query/"+row.Name, func(t *testing.T) {
			got, err := kernel.Query(compactJSON(t, row.Query), compactJSON(t, row.Documents))
			if goldenError(t, err, row.Error) {
				return
			}
			if !reflect.DeepEqual(jsonValue(t, []byte(got)), jsonValue(t, row.Expected)) {
				t.Fatalf("page %s want %s", got, row.Expected)
			}
		})
	}
}

// TestFloatGoldenThroughTheSerializer replays every filter, query and cursor row that holds a float
// with the float as a float64, so the SDK's serializer, not the golden text, spells it.
func TestFloatGoldenThroughTheSerializer(t *testing.T) {
	for _, row := range goldenRows[struct {
		Name     string
		Document json.RawMessage
		Filter   json.RawMessage
		Expected *bool
		Error    *string
	}](t, "filter") {
		document, filter := hostJSON(t, row.Document), hostJSON(t, row.Filter)
		if !holdsFloat(document) && !holdsFloat(filter) {
			continue
		}
		t.Run("filter/"+row.Name, func(t *testing.T) {
			got, err := Filter(filter.(map[string]any)).Matches(document.(map[string]any))
			if goldenError(t, err, row.Error) {
				return
			}
			if got != *row.Expected {
				t.Fatalf("got %v want %v", got, *row.Expected)
			}
		})
	}
	for _, row := range goldenRows[struct {
		Name      string
		Documents json.RawMessage
		Query     json.RawMessage
		Expected  json.RawMessage `json:"expected_page"`
		Error     *string
	}](t, "query") {
		documents := hostJSON(t, row.Documents).([]any)
		if !holdsFloat(documents) {
			continue
		}
		t.Run("query/"+row.Name, func(t *testing.T) {
			var spec QuerySpec
			if err := decodeJSON(row.Query, &spec); err != nil {
				t.Fatal(err)
			}
			rows := make([]Document, len(documents))
			for i, document := range documents {
				rows[i] = document.(map[string]any)
			}
			page, err := spec.Apply(rows)
			if goldenError(t, err, row.Error) {
				return
			}
			got, err := encodeJSON(page)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(hostJSON(t, got), hostJSON(t, row.Expected)) {
				t.Fatalf("page %s want %s", got, row.Expected)
			}
		})
	}
	for _, row := range goldenRows[struct {
		Document       json.RawMessage
		Sort           []SortKey
		ExpectedCursor *string `json:"expected_cursor"`
	}](t, "cursor") {
		document, ok := hostJSON(t, row.Document).(map[string]any)
		if !ok || !holdsFloat(document) {
			continue
		}
		t.Run(fmt.Sprint("cursor/", document["id"]), func(t *testing.T) {
			// A second row after this one makes the page stop here and mint the cursor.
			next := maps.Clone(document)
			next["id"] = fmt.Sprint(document["id"], "~")
			page, err := QuerySpec{Sort: row.Sort, Limit: Ptr(uint64(1))}.Apply([]Document{next, document})
			if err != nil || page.Next == nil || *page.Next != *row.ExpectedCursor {
				t.Fatalf("next %v want %s: %v", page.Next, *row.ExpectedCursor, err)
			}
		})
	}
}

func hostJSON(t *testing.T, raw []byte) any {
	t.Helper()
	value, err := materializeHost(jsonValue(t, raw))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func holdsFloat(value any) bool {
	switch item := value.(type) {
	case float64:
		return true
	case map[string]any:
		for _, child := range item {
			if holdsFloat(child) {
				return true
			}
		}
	case []any:
		return slices.ContainsFunc(item, holdsFloat)
	}
	return false
}

func TestRetrySequenceGolden(t *testing.T) {
	for _, row := range goldenRows[struct {
		Name     string
		Attempts uint32
		Failed   uint32
		Code     *string
		Want     *uint64
		Error    *string
	}](t, "retry_step") {
		t.Run("retry/"+row.Name, func(t *testing.T) {
			got, err := kernel.RetryStep(row.Attempts, row.Failed, row.Code)
			if goldenError(t, err, row.Error) {
				return
			}
			if !reflect.DeepEqual(got, row.Want) {
				t.Fatalf("step %v want %v", got, row.Want)
			}
		})
	}
	for index, row := range goldenRows[struct {
		Expected  uint64
		Sequences []*uint64
		Version   *uint64
		Error     *string
	}](t, "sequence_batch") {
		t.Run(fmt.Sprintf("sequence/%d", index), func(t *testing.T) {
			got, err := kernel.CheckSequenceBatch(row.Expected, row.Sequences)
			if goldenError(t, err, row.Error) {
				return
			}
			if got != *row.Version {
				t.Fatalf("version %d want %d", got, *row.Version)
			}
		})
	}
	for _, row := range goldenRows[struct {
		Code     string
		Wire     string
		Terminal bool
		Fatal    bool
	}](t, "error_taxonomy") {
		t.Run("taxonomy/"+row.Code, func(t *testing.T) {
			err := ErrorFromWire(row.Wire)
			if IsTerminal(err) != row.Terminal || IsFatal(err) != row.Fatal {
				t.Fatalf("verdict terminal=%v fatal=%v", IsTerminal(err), IsFatal(err))
			}
		})
	}
}

func TestNilNativeCollections(t *testing.T) {
	draft := EventDraft{ID: "event-empty-bytes", AggregateID: "invoice-42", Name: "InvoiceIssued", Version: 1, Sequence: Ptr(uint64(1)), CorrelationID: "flow-42", CausationID: "cmd-42"}
	message, err := kernel.RecordEvent(draft, 123)
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"json", "msgpack"} {
		wire, err := EncodeEvent(format, *message)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeEvent(format, wire)
		if err != nil || len(decoded.Data) != 0 {
			t.Fatalf("empty data: %+v %v", decoded, err)
		}
	}
	if draft.Data != nil || draft.Extensions != nil || draft.Metadata != nil {
		t.Fatal("binding changed caller's zero-value collections")
	}
	batch, err := kernel.GetDraftBatch("invoice-42", 0, 1, nil, nil, Lineage{CorrelationID: "flow-42", CausationID: "cmd-42"}, nil, nil, nil, nil)
	if err != nil || len(batch.Drafts) != 0 || batch.Version != 0 {
		t.Fatalf("empty draft batch: %+v %v", batch, err)
	}
}

func compactJSON(t *testing.T, raw []byte) string {
	t.Helper()
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		t.Fatal(err)
	}
	return compact.String()
}

// materializeHost decodes golden JSON values as a host holds them: a float literal as a float64,
// and the `$decimal` and `$instant` objects as Decimal and Instant.
func materializeHost(value any) (any, error) {
	switch item := value.(type) {
	case json.Number:
		if strings.ContainsAny(string(item), ".eE") {
			return item.Float64()
		}
	case map[string]any:
		if len(item) == 1 {
			if text, ok := item["$decimal"].(string); ok {
				return NewDecimal(text)
			}
			if text, ok := item["$instant"].(string); ok {
				return NewInstant(text)
			}
		}
		for key, child := range item {
			var err error
			item[key], err = materializeHost(child)
			if err != nil {
				return nil, err
			}
		}
	case []any:
		for index, child := range item {
			var err error
			item[index], err = materializeHost(child)
			if err != nil {
				return nil, err
			}
		}
	}
	return value, nil
}

func TestUpcastGolden(t *testing.T) {
	for index, row := range goldenRows[struct {
		Available   []uint32
		From        uint32
		To          uint32
		Plan        []uint32
		Error       *string
		Target      *uint32
		TargetError *string `json:"target_error"`
	}](t, "upcast") {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			plan, err := kernel.UpcastPlan(row.From, row.To, row.Available)
			if !goldenError(t, err, row.Error) && !reflect.DeepEqual(plan, row.Plan) {
				t.Fatalf("plan %#v want %#v", plan, row.Plan)
			}
			target, err := kernel.UpcastTarget(row.Available)
			if !goldenError(t, err, row.TargetError) && target != *row.Target {
				t.Fatalf("target %d want %d", target, *row.Target)
			}
		})
	}
}

// The ceiling pairs pin the full uint64 range: a sentinel for an absent mark would break them.
func TestStalenessGolden(t *testing.T) {
	for index, row := range goldenRows[struct {
		Op        string
		Name      string
		Stored    *uint64
		Incoming  *uint64
		Stale     bool
		Advanced  *uint64
		Stream    []*uint64
		Applied   []*uint64
		Watermark *uint64
	}](t, "staleness") {
		name := row.Name
		if name == "" {
			name = fmt.Sprint(index)
		}
		t.Run(row.Op+"/"+name, func(t *testing.T) {
			if row.Op == "pair" {
				if got := IsStale(row.Stored, row.Incoming); got != row.Stale {
					t.Fatalf("stale %v want %v", got, row.Stale)
				}
				if got := Advance(row.Stored, row.Incoming); !reflect.DeepEqual(got, row.Advanced) {
					t.Fatalf("advanced %v want %v", markText(got), markText(row.Advanced))
				}
				return
			}
			var watermark *uint64
			applied := []*uint64{}
			for _, incoming := range row.Stream {
				if !IsStale(watermark, incoming) {
					applied = append(applied, incoming)
					watermark = Advance(watermark, incoming)
				}
			}
			if !reflect.DeepEqual(applied, row.Applied) || !reflect.DeepEqual(watermark, row.Watermark) {
				t.Fatalf("watermark %v", markText(watermark))
			}
		})
	}
}

func markText(mark *uint64) string {
	if mark == nil {
		return "nil"
	}
	return fmt.Sprint(*mark)
}

func TestRedisAbsentAndEmptyBytes(t *testing.T) {
	claim, err := kernel.RedisNewClaim(42)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		reply  []byte
		won    bool
		remove bool
	}{
		{"absent", nil, true, false},
		{"empty_corrupt", []byte{}, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := kernel.RedisClaimOutcome(claim, test.reply)
			if err != nil {
				t.Fatal(err)
			}
			if result.Won != test.won {
				t.Fatalf("won=%v want %v", result.Won, test.won)
			}
			if !test.won {
				if result.Prior == nil {
					t.Fatal("lost claim has no unreadable prior")
				}
				prior, err := kernel.DecodePriorIdempotencyRecord(*result.Prior, 42)
				if err != nil {
					t.Fatal(err)
				}
				if prior.State != "failed" || prior.ErrorCode == nil || *prior.ErrorCode != "codec" {
					t.Fatalf("empty record %+v", prior)
				}
			}
			remove, err := kernel.RedisForgetOutcome("invoice-42", test.reply)
			if err != nil || remove != test.remove {
				t.Fatalf("forget=%v want %v error=%v", remove, test.remove, err)
			}
		})
	}
}

// goldenLedger is the memory ledger with its raw-record test hook, so a golden prior lands byte for
// byte. This file is package edax, so it cannot import backend/memory, which imports edax.
type goldenLedger struct {
	native *kernel.MemoryIdempotencyStore
}

func (l goldenLedger) Claim(_ context.Context, key string, hash uint64) (*ClaimResult, error) {
	return l.native.Claim(kernel.NoTx(), key, hash)
}
func (l goldenLedger) Complete(_ context.Context, key string, handle []byte, record IdempotencyRecord) error {
	wire, err := EncodeRecord(record)
	if err != nil {
		return err
	}
	return l.native.Complete(kernel.NoTx(), key, handle, wire)
}
func (l goldenLedger) Release(_ context.Context, key string, handle []byte) error {
	return l.native.Release(kernel.NoTx(), key, handle)
}
func (l goldenLedger) Forget(_ context.Context, key string) error {
	return l.native.Forget(kernel.NoTx(), key)
}

// guardScenario is one golden row as the guard meets it: what the key holds before, what the handler
// does when it runs, and what the dispatch returns and the key holds after.
type guardScenario struct {
	name       string
	hash       uint64
	prior      *string
	run        func() (any, error) // nil for a loser, which must not run the handler
	wantResult any
	wantError  *string
	wantRecord *string
}

// goldenFailure is a handler failure as the golden files spell it: a code, or none, and a message.
func goldenFailure(code *string, message string) (wire string, failure error) {
	if code == nil {
		return message, errors.New(message)
	}
	wire = *code + ": " + message
	return wire, ErrorFromWire(wire)
}

// TestIdempotencyGoldenThroughTheGuard replays the guard, settle and record vectors through Guard,
// the engine Idempotent and IdempotentByID delegate to, over the memory ledger. The rows fix the
// ledger key and hash, which the middlewares derive from a message, so they enter at Guard.
func TestIdempotencyGoldenThroughTheGuard(t *testing.T) {
	const key = "req-1"
	var scenarios []guardScenario
	for _, row := range goldenRows[struct {
		Name        string
		Key         string
		PayloadHash uint64 `json:"payload_hash"`
		Prior       *string
		Run         json.RawMessage
		Want        json.RawMessage
		WantRecord  *string `json:"want_record"`
	}](t, "idempotency_guard") {
		if row.Key != key {
			t.Fatalf("%s: key %q", row.Name, row.Key)
		}
		scenario := guardScenario{name: "guard/" + row.Name, hash: row.PayloadHash, prior: row.Prior, wantRecord: row.WantRecord}
		if run, ok := jsonValue(t, row.Run).(map[string]any); ok {
			scenario.run = func() (any, error) {
				if result, ok := run["result"]; ok {
					return result, nil
				}
				failure := run["error"].(map[string]any)
				var code *string
				if text, ok := failure["code"].(string); ok {
					code = &text
				}
				_, err := goldenFailure(code, failure["message"].(string))
				return nil, err
			}
		}
		want := jsonValue(t, row.Want).(map[string]any)
		if text, ok := want["error"].(string); ok {
			scenario.wantError = &text
		} else {
			scenario.wantResult = want["result"]
		}
		scenarios = append(scenarios, scenario)
	}
	for _, row := range goldenRows[struct {
		Code        *string
		Message     string
		PayloadHash uint64 `json:"payload_hash"`
		Want        json.RawMessage
	}](t, "idempotency_settle") {
		wire, failure := goldenFailure(row.Code, row.Message)
		scenario := guardScenario{name: fmt.Sprintf("settle/%s", wire), hash: row.PayloadHash, run: func() (any, error) { return nil, failure }, wantError: &wire}
		if cache, ok := jsonValue(t, row.Want).(map[string]any); ok {
			record := cache["cache"].(string)
			scenario.wantRecord = &record
		}
		scenarios = append(scenarios, scenario)
	}
	for _, row := range goldenRows[struct {
		Name        string
		Stored      string
		Current     uint64
		ReplayError *string `json:"replay_error"`
		Result      json.RawMessage
	}](t, "idempotency_record") {
		scenario := guardScenario{name: "record/" + row.Name, hash: row.Current, prior: &row.Stored, wantError: row.ReplayError, wantRecord: &row.Stored}
		if row.ReplayError == nil && len(row.Result) > 0 {
			scenario.wantResult = jsonValue(t, row.Result)
		}
		scenarios = append(scenarios, scenario)
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			database := kernel.GetMemoryTransactor()
			defer database.Close()
			native, err := database.IdempotencyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer native.Close()
			ledger := goldenLedger{native}
			if scenario.prior != nil {
				if err = native.PutRaw(key, *scenario.prior); err != nil {
					t.Fatal(err)
				}
			}
			runs := 0
			got, err := Guard(ctx, ledger, key, scenario.hash, func(context.Context) (any, error) {
				runs++
				if scenario.run == nil {
					return nil, errors.New("a loser ran the handler")
				}
				return scenario.run()
			})
			if wantRuns := map[bool]int{false: 0, true: 1}[scenario.run != nil]; runs != wantRuns {
				t.Fatalf("handler ran %d times, want %d", runs, wantRuns)
			}
			if scenario.wantError != nil {
				if err == nil || err.Error() != *scenario.wantError {
					t.Fatalf("error %v, want %q", err, *scenario.wantError)
				}
			} else if err != nil || !reflect.DeepEqual(got, scenario.wantResult) {
				t.Fatalf("result %#v, want %#v: %v", got, scenario.wantResult, err)
			}
			// What the key holds: a fresh claim wins once the claim was released, else reads the bytes.
			held, err := ledger.Claim(ctx, key, scenario.hash)
			if err != nil {
				t.Fatal(err)
			}
			var holds *string
			if !held.Won {
				holds = held.Prior
			}
			if !reflect.DeepEqual(holds, scenario.wantRecord) {
				t.Fatalf("key holds %v, want %v", markTextOf(holds), markTextOf(scenario.wantRecord))
			}
		})
	}
}

func markTextOf(text *string) string {
	if text == nil {
		return "nothing"
	}
	return *text
}
