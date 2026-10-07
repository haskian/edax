package redis

import (
	"context"
	"errors"
	"sync"

	edax "github.com/haskian/edax/crates/edax-go"
	goredis "github.com/redis/go-redis/v9"
)

// Client adapts a caller-owned go-redis client. Configure ContextTimeoutEnabled
// on that client so the store's deadline bounds socket reads and writes.
// The adapter supports Redis and Redis Cluster and never closes the client.
type Client struct {
	client  goredis.UniversalClient
	scripts sync.Map
}

// NewClient adapts a caller-owned go-redis client without opening another connection.
func NewClient(client goredis.UniversalClient) *Client { return &Client{client: client} }

// Eval runs a cached Lua script and retries with its source after NOSCRIPT.
func (c *Client) Eval(ctx context.Context, script string, keys [][]byte, args ...any) (any, error) {
	cached, _ := c.scripts.LoadOrStore(script, goredis.NewScript(script))
	names := make([]string, len(keys))
	for i, key := range keys {
		names[i] = string(key)
	}
	reply, err := cached.(*goredis.Script).Run(ctx, c.client, names, args...).Result()
	return reply, classify(err)
}

// Get reads raw ledger bytes; a missing key returns nil without an error.
func (c *Client) Get(ctx context.Context, key []byte) ([]byte, error) {
	reply, err := c.client.Get(ctx, string(key)).Bytes()
	return reply, classify(err)
}

func classify(err error) error {
	if errors.Is(err, goredis.Nil) {
		return nil
	}
	var server interface {
		error
		RedisError()
	}
	if errors.As(err, &server) {
		failure := ReplyError(server.Error())
		var typed *edax.Error
		if errors.As(failure, &typed) {
			return edax.WrapError(typed.Code, typed.Message, err)
		}
		return failure
	}
	return err
}
