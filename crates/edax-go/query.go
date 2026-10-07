package edax

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Document is a JSON read model. Decoding retains integers as json.Number.
type Document map[string]any

// Filter is the kernel's backend-neutral filter tree. Constructors only build its wire shape.
type Filter map[string]any

type comparison struct {
	Path      string `json:"path"`
	Op        string `json:"op"`
	Value     any    `json:"value"`
	Collation string `json:"collation,omitempty"`
}

func compare(path, operator string, value any) Filter {
	collation := ""
	if _, ok := value.(Decimal); ok {
		collation = "decimal"
	}
	return Filter{"compare": comparison{path, operator, value, collation}}
}

// Equal compares a document path for equality using kernel value semantics.
func Equal(path string, value any) Filter { return compare(path, "eq", value) }

// NotEqual compares a document path for inequality using kernel value semantics.
func NotEqual(path string, value any) Filter { return compare(path, "ne", value) }

// Greater compares a document path with an exclusive lower bound.
func Greater(path string, value any) Filter { return compare(path, "gt", value) }

// GreaterEqual compares a document path with an inclusive lower bound.
func GreaterEqual(path string, value any) Filter { return compare(path, "gte", value) }

// Less compares a document path with an exclusive upper bound.
func Less(path string, value any) Filter { return compare(path, "lt", value) }

// LessEqual compares a document path with an inclusive upper bound.
func LessEqual(path string, value any) Filter { return compare(path, "lte", value) }

// StartsWith matches string values at path with the supplied prefix.
func StartsWith(path, prefix string) Filter { return compare(path, "starts_with", prefix) }

// Contains tests membership in the array at path using kernel value semantics.
func Contains(path string, value any) Filter {
	return Filter{"contains": map[string]any{"path": path, "value": value}}
}

// Exists tests whether path is present and non-null; false matches missing or null values.
func Exists(path string, present bool) Filter {
	return Filter{"exists": map[string]any{"path": path, "exists": present}}
}

// In matches path against any supplied value; Decimal values select decimal collation.
func In(path string, values ...any) Filter {
	if values == nil {
		values = []any{}
	}
	node := map[string]any{"path": path, "values": values}
	for _, value := range values {
		if _, ok := value.(Decimal); ok {
			node["collation"] = "decimal"
		}
	}
	return Filter{"in": node}
}

// And requires every child filter to match; no children match every document.
func And(filters ...Filter) Filter {
	if filters == nil {
		filters = []Filter{}
	}
	return Filter{"and": filters}
}

// Or requires at least one child filter to match; no children match no documents.
func Or(filters ...Filter) Filter {
	if filters == nil {
		filters = []Filter{}
	}
	return Filter{"or": filters}
}

// Not negates a filter using kernel semantics.
func Not(filter Filter) Filter { return Filter{"not": filter} }

// Matches validates and evaluates this filter against a document in the kernel.
func (f Filter) Matches(document Document) (bool, error) {
	filter, err := marshalText(f)
	if err != nil {
		return false, err
	}
	doc, err := marshalText(document)
	if err != nil {
		return false, err
	}
	return kernel.EvalFilter(filter, doc)
}

// SortKey selects a path, direction and optional decimal collation.
type SortKey struct {
	Path      string `json:"path"`
	Desc      bool   `json:"desc,omitempty"`
	Collation string `json:"collation,omitempty"`
}

// QuerySpec selects a page. A nil limit is unbounded; a pointer to zero returns no rows.
type QuerySpec struct {
	Filter Filter    `json:"filter,omitempty"`
	Sort   []SortKey `json:"sort,omitempty"`
	Offset uint64    `json:"offset,omitempty"`
	Limit  *uint64   `json:"limit,omitempty"`
	After  *string   `json:"after,omitempty"`
}

// Page carries a cursor only when more rows follow.
type Page struct {
	Rows []Document `json:"rows"`
	Next *string    `json:"next"`
}

// JSON validates the query in the kernel and returns its backend wire representation.
func (q QuerySpec) JSON() (string, error) {
	wire, err := marshalText(q)
	if err != nil {
		return "", err
	}
	if err = kernel.CheckQuery(wire); err != nil {
		return "", err
	}
	return wire, nil
}

// Apply evaluates this query over supplied documents and preserves numbers as json.Number.
func (q QuerySpec) Apply(documents []Document) (Page, error) {
	wire, err := q.JSON()
	if err != nil {
		return Page{}, err
	}
	if documents == nil {
		documents = []Document{}
	}
	docs, err := marshalText(documents)
	if err != nil {
		return Page{}, err
	}
	result, err := kernel.Query(wire, docs)
	if err != nil {
		return Page{}, err
	}
	var page Page
	err = decodeJSON([]byte(result), &page)
	return page, err
}

// Entity is a read model at the version last read from its store.
type Entity struct {
	ID       string
	Version  uint64
	Document Document
}

// LoadEntity reads a projection and its version. An absent row returns nil without an error.
func LoadEntity(ctx context.Context, store ProjectionStore, id string) (*Entity, error) {
	row, err := store.Get(ctx, id)
	if err != nil || row == nil {
		return nil, err
	}
	var document Document
	if err = decodeJSON([]byte(row.DocumentJSON), &document); err != nil {
		return nil, err
	}
	return &Entity{ID: id, Version: row.Version, Document: document}, nil
}

// SaveEntity writes the document at its expected version and returns the new version.
// The supplied Entity is not modified.
func SaveEntity(ctx context.Context, store ProjectionStore, entity Entity) (uint64, error) {
	wire, err := marshalText(entity.Document)
	if err != nil {
		return 0, err
	}
	return store.Save(ctx, entity.ID, entity.Version, wire)
}

// QueryEntities runs a validated query through the store and decodes its page.
func QueryEntities(ctx context.Context, store ProjectionStore, spec QuerySpec) (Page, error) {
	wire, err := spec.JSON()
	if err != nil {
		return Page{}, err
	}
	result, err := store.GetMany(ctx, wire)
	if err != nil {
		return Page{}, err
	}
	var page Page
	err = decodeJSON([]byte(result), &page)
	return page, err
}
func marshalText(value any) (string, error) {
	wire, err := json.Marshal(value)
	if err != nil {
		return "", &Error{Code: CodeCodec, Message: err.Error()}
	}
	return string(wire), nil
}
func decodeJSON(wire []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return &Error{Code: CodeCodec, Message: err.Error()}
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return &Error{Code: CodeCodec, Message: "expected exactly one JSON value"}
	}
	return nil
}
