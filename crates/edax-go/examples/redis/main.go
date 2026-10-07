// Run against a local Redis server: go run ./examples/redis.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	redis "github.com/haskian/edax/crates/edax-go/backend/redis"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379", ContextTimeoutEnabled: true})
	defer client.Close()
	store, err := redis.New(redis.NewClient(client), redis.Config{Prefix: "edax:example:billing:", TTL: time.Minute})
	if err != nil {
		return err
	}
	defer store.Close()
	payload := []byte(`{"invoice_id":"1042","total":"49.90","currency":"EUR"}`)
	hash, err := kernel.CommandHashJSON("IssueInvoice", payload)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("invoice:1042:%d", time.Now().UnixNano())
	calls := 0
	handler := func(context.Context) (any, error) {
		calls++
		return map[string]any{"invoice_id": "1042", "accepted": true}, nil
	}
	for range 2 {
		result, failure := edax.Guard(ctx, store, key, hash, handler)
		if failure != nil {
			return failure
		}
		fmt.Printf("invoice result: %v\n", result)
	}
	if calls != 1 {
		return fmt.Errorf("handler called %d times, expected one", calls)
	}
	return store.Forget(ctx, key)
}
