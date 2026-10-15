// Filter and sort a read model, then follow a cursor across a tied sort value.
// Run from crates/edax-go: mise exec -- go run ./examples/11_projection_query
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok: cursor preserved both sides of the tie and refused a different sort")
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db := memory.New()
	defer func() { err = errors.Join(err, db.Close()) }()
	store, err := db.ProjectionStore("accounts")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	accounts := []struct {
		id, owner string
		balance   int
		status    string
	}{
		{"a1", "Ann", 300, "active"}, {"a2", "Bob", 150, "active"},
		{"a3", "Cid", 150, "active"}, {"a4", "Dee", 50, "active"},
		{"a5", "Eve", 500, "closed"}, {"a6", "Fay", 100, "active"},
	}
	for _, account := range accounts {
		entity := edax.Entity{ID: account.id, Document: edax.Document{"owner": account.owner, "balance": account.balance, "status": account.status}}
		if _, err := edax.SaveEntity(ctx, store, entity); err != nil {
			return err
		}
	}
	query := edax.QuerySpec{
		Filter: edax.And(edax.GreaterEqual("balance", 100), edax.Equal("status", "active")),
		Sort:   []edax.SortKey{{Path: "balance", Desc: true}, {Path: "id"}}, Limit: edax.Ptr(uint64(2)),
	}
	var pages [][]string
	var firstCursor *string
	for {
		page, err := edax.QueryEntities(ctx, store, query)
		if err != nil {
			return err
		}
		var ids []string
		for _, row := range page.Rows {
			id, ok := row["id"].(string)
			if !ok {
				return fmt.Errorf("missing document id: %v", row)
			}
			ids = append(ids, id)
		}
		pages = append(pages, ids)
		fmt.Printf("page %d: %v\n", len(pages), ids)
		if firstCursor == nil {
			firstCursor = page.Next
		}
		if page.Next == nil {
			break
		}
		query.After = page.Next
	}
	if !reflect.DeepEqual(pages, [][]string{{"a1", "a2"}, {"a3", "a6"}}) {
		return fmt.Errorf("unexpected pages: %v", pages)
	}
	if firstCursor == nil {
		return errors.New("first page did not return a cursor")
	}
	query.Sort, query.After = []edax.SortKey{{Path: "id"}}, firstCursor
	if _, err := edax.QueryEntities(ctx, store, query); !errors.Is(err, edax.ErrCodec) {
		return fmt.Errorf("expected cursor sort mismatch, got %v", err)
	}
	return nil
}
