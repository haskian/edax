package edax

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
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
				raw, encodeErr := json.Marshal(value)
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

func materializeHost(value any) (any, error) {
	switch item := value.(type) {
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
