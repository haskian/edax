// Package redis implements the shared idempotency ledger with the kernel's Lua.
// The caller owns the connection and must honor each operation's context.
package redis

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// Conn executes the unmodified kernel scripts and reads a ledger value.
// Eval replies are nil, []byte, string or int64. GET returns nil for a missing key.
// Implementations must honor cancellation and preserve Redis error prefixes.
type Conn interface {
	Eval(context.Context, string, [][]byte, ...any) (any, error)
	Get(context.Context, []byte) ([]byte, error)
}

// Config controls the ledger's namespace, retention and per-operation deadline.
// Zero durations select kernel defaults. Prefix is required.
type Config struct {
	// Logger receives renewal diagnostics. Nil captures slog.Default at construction.
	Logger    *slog.Logger
	Prefix    string
	TTL       time.Duration
	ClaimTTL  time.Duration
	OpTimeout time.Duration
	Heartbeat bool
	// LeaseLost runs after its renewal stops. Close does not wait for it, so
	// the callback may call Close.
	LeaseLost func(key string, failure error)
}

// Store fences every write with the kernel-minted claim handle. It cannot join
// another database's transaction. Close stops renewals without closing Conn.
type Store struct {
	conn     Conn
	config   Config
	scripts  *kernel.RedisScripts
	mu       sync.Mutex
	closed   bool
	renewals map[string]context.CancelFunc
	workers  sync.WaitGroup
}

// New validates the ledger configuration and wraps a caller-owned connection.
func New(conn Conn, config Config) (*Store, error) {
	if conn == nil {
		return nil, &edax.Error{Code: edax.CodeBadHandle, Message: "Redis connection is nil"}
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	defaults := kernel.GetRedisDefaults()
	if config.TTL == 0 {
		config.TTL = time.Duration(defaults.TTLMs) * time.Millisecond
	}
	if config.ClaimTTL == 0 {
		config.ClaimTTL = time.Duration(defaults.ClaimTTLMs) * time.Millisecond
	}
	if config.OpTimeout == 0 {
		config.OpTimeout = time.Duration(defaults.OpTimeoutMs) * time.Millisecond
	}
	// The kernel validates whole milliseconds. Round the deadline up, so each
	// call applies exactly the budget the kernel checked against the lease.
	if whole := config.OpTimeout.Truncate(time.Millisecond); whole < config.OpTimeout {
		config.OpTimeout = whole + time.Millisecond
	}
	if err := kernel.RedisCheckConfig(config.Prefix, config.TTL.Milliseconds(), config.ClaimTTL.Milliseconds(), config.OpTimeout.Milliseconds(), config.Heartbeat); err != nil {
		return nil, err
	}
	return &Store{conn: conn, config: config, scripts: kernel.GetRedisScripts(), renewals: make(map[string]context.CancelFunc)}, nil
}

func (s *Store) call(ctx context.Context, key string, op func(context.Context, []byte) (any, error)) (any, error) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, &edax.Error{Code: edax.CodeBadHandle, Message: "Redis store is closed"}
	}
	rkey, err := kernel.RedisLedgerKey(s.config.Prefix, key)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.OpTimeout)
	defer cancel()
	reply, err := op(ctx, rkey)
	if err == nil {
		return reply, nil
	}
	if code := kernel.CodeOf(err); code != nil {
		return nil, edax.WrapError(edax.ErrorCode(*code), fmt.Sprintf("redis %q: %v", key, err), err)
	}
	// Context errors stay unwrap-able for caller cancellation checks.
	return nil, edax.WrapError(edax.CodeStore, fmt.Sprintf("redis %q: %v", key, err), err)
}

func (s *Store) eval(ctx context.Context, key, script string, args ...any) (any, error) {
	return s.call(ctx, key, func(ctx context.Context, rkey []byte) (any, error) {
		return s.conn.Eval(ctx, script, [][]byte{rkey}, args...)
	})
}

// Claim acquires a fenced lease or returns the existing settled record.
// When enabled, renewal starts only for a newly acquired lease.
func (s *Store) Claim(ctx context.Context, key string, hash uint64) (*edax.ClaimResult, error) {
	claim, err := kernel.RedisNewClaim(hash)
	if err != nil {
		return nil, err
	}
	sentAt := time.Now()
	reply, err := s.eval(ctx, key, s.scripts.Claim, claim, s.config.ClaimTTL.Milliseconds())
	if err != nil {
		return nil, err
	}
	held, err := replyBytes(reply)
	if err != nil {
		return nil, err
	}
	result, err := kernel.RedisClaimOutcome(claim, held)
	if err != nil {
		return nil, err
	}
	if result.Won && s.config.Heartbeat {
		renewal, cancel := context.WithCancel(context.Background())
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			cancel()
			return nil, &edax.Error{Code: edax.CodeBadHandle, Message: "Redis store closed during claim"}
		}
		s.renewals[string(result.Handle)] = cancel
		s.workers.Add(1)
		s.mu.Unlock()
		go s.renew(renewal, key, result.Handle, sentAt)
	}
	return result, nil
}

// Complete stops renewal and settles the record only while the handle still owns it.
func (s *Store) Complete(ctx context.Context, key string, handle []byte, record edax.IdempotencyRecord) error {
	body, err := edax.EncodeRecord(record)
	if err != nil {
		return err
	}
	value, err := kernel.RedisSettledValue(handle, body)
	if err != nil {
		return err
	}
	s.stop(handle)
	reply, err := s.eval(ctx, key, s.scripts.Complete, handle, value, s.config.TTL.Milliseconds())
	if err != nil {
		return err
	}
	written, err := replyBool(reply)
	if err != nil {
		return err
	}
	return kernel.RedisCompleteOutcome(key, written)
}

// Release stops renewal and removes a pending claim only while its handle owns it.
func (s *Store) Release(ctx context.Context, key string, handle []byte) error {
	if err := kernel.RedisCheckClaim(handle); err != nil {
		return err
	}
	s.stop(handle)
	_, err := s.eval(ctx, key, s.scripts.Release, handle)
	return err
}

// Extend renews a matching pending claim and reports false after lease loss.
func (s *Store) Extend(ctx context.Context, key string, handle []byte) (bool, error) {
	if err := kernel.RedisCheckClaim(handle); err != nil {
		return false, err
	}
	reply, err := s.eval(ctx, key, s.scripts.Extend, handle, s.config.ClaimTTL.Milliseconds())
	if err != nil {
		return false, err
	}
	return replyBool(reply)
}

// Forget removes a settled record and refuses an active claim.
func (s *Store) Forget(ctx context.Context, key string) error {
	for attempt := uint32(0); attempt < kernel.RedisForgetReads(); attempt++ {
		reply, err := s.call(ctx, key, func(ctx context.Context, rkey []byte) (any, error) { return s.conn.Get(ctx, rkey) })
		if err != nil {
			return err
		}
		held, err := replyBytes(reply)
		if err != nil {
			return err
		}
		deletes, err := kernel.RedisForgetOutcome(key, held)
		if err != nil {
			return err
		}
		if !deletes {
			return nil
		}
		reply, err = s.eval(ctx, key, s.scripts.Forget, held)
		if err != nil {
			return err
		}
		deleted, err := replyBool(reply)
		if err != nil {
			return err
		}
		if deleted {
			return nil
		}
	}
	wire, err := kernel.RedisForgetExhausted(key)
	if err != nil {
		return err
	}
	return edax.ErrorFromWire(wire)
}

func (s *Store) renew(ctx context.Context, key string, handle []byte, sentAt time.Time) {
	lost, failure := s.heartbeat(ctx, key, handle, sentAt)
	s.mu.Lock()
	delete(s.renewals, string(handle))
	s.mu.Unlock()
	// Release the worker before notifying, so the callback may call Close.
	s.workers.Done()
	if lost && s.config.LeaseLost != nil {
		s.config.LeaseLost(key, failure)
	}
}

// heartbeat extends the claim until cancellation or lease loss.
func (s *Store) heartbeat(ctx context.Context, key string, handle []byte, sentAt time.Time) (lost bool, failure error) {
	for beat := uint32(1); ; beat++ {
		due := kernel.RedisHeartbeatDueMs(0, s.config.ClaimTTL.Milliseconds(), beat)
		timer := time.NewTimer(time.Until(sentAt.Add(time.Duration(due) * time.Millisecond)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, nil
		case <-timer.C:
		}
		renewed, err := s.Extend(ctx, key, handle)
		if ctx.Err() != nil {
			return false, nil
		}
		if err != nil && !edax.IsTerminal(err) {
			s.config.Logger.WarnContext(ctx, "edax Redis claim renewal failed", "key", key, "error", err)
			continue
		}
		if err != nil || !renewed {
			s.config.Logger.WarnContext(ctx, "edax Redis claim lease lost", "key", key, "error", err)
			return true, err
		}
	}
}

func (s *Store) stop(handle []byte) {
	s.mu.Lock()
	cancel := s.renewals[string(handle)]
	delete(s.renewals, string(handle))
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Close waits for renewal calls to stop. Conn must respect their cancellation.
func (s *Store) Close() error {
	s.mu.Lock()
	s.closed = true
	for _, cancel := range s.renewals {
		cancel()
	}
	s.mu.Unlock()
	s.workers.Wait()
	return nil
}

func replyBytes(reply any) ([]byte, error) {
	switch value := reply.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	default:
		return nil, &edax.Error{Code: edax.CodeCodec, Message: fmt.Sprintf("unexpected Redis value reply %T", reply)}
	}
}
func replyBool(reply any) (bool, error) {
	switch value := reply.(type) {
	case int64:
		return value != 0, nil
	case int:
		return value != 0, nil
	case nil:
		return false, nil
	default:
		return false, &edax.Error{Code: edax.CodeCodec, Message: fmt.Sprintf("unexpected Redis script reply %T", reply)}
	}
}

// ReplyError classifies a Redis server reply through the kernel. Pass its full
// prefix, such as WRONGTYPE or READONLY. Connection errors belong to Conn.
func ReplyError(reply string) error {
	wire, err := kernel.RedisReplyError(reply)
	if err != nil {
		return err
	}
	return edax.ErrorFromWire(wire)
}
