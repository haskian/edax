package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	edax "github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
	goredis "github.com/redis/go-redis/v9"
)

func ledger(t *testing.T) (*Store, *miniredis.Miniredis, *goredis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close() })
	store, err := New(NewClient(client), Config{Prefix: "billing:test:idem:", TTL: time.Hour, ClaimTTL: time.Minute, OpTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, server, client
}
func TestKernelLuaLedger(t *testing.T) {
	for _, name := range []string{"replay", "release", "stale_complete", "stale_release", "forget_pending", "forget_settled", "forget_absent", "corrupt_record", "claim_expiry", "wrong_type"} {
		t.Run(name, func(t *testing.T) {
			store, server, client := ledger(t)
			ctx := context.Background()
			key := "invoice:ž-1042"
			claim, err := store.Claim(ctx, key, 42)
			if err != nil || !claim.Won {
				t.Fatalf("claim: %+v %v", claim, err)
			}
			result := `{"invoice_id":"ž-1042","accepted":true}`
			record := edax.IdempotencyRecord{PayloadHash: 42, State: "done", ResultJSON: &result}
			switch name {
			case "replay":
				if err = store.Complete(ctx, key, claim.Handle, record); err != nil {
					t.Fatal(err)
				}
				prior, failure := store.Claim(ctx, key, 42)
				if failure != nil || prior.Won || prior.Prior == nil {
					t.Fatalf("prior: %+v %v", prior, failure)
				}
				decoded, failure := kernel.DecodeIdempotencyRecord(*prior.Prior)
				if failure != nil || decoded.ResultJSON == nil {
					t.Fatalf("decoded: %+v %v", decoded, failure)
				}
				var cached struct {
					InvoiceID string `json:"invoice_id"`
					Accepted  bool   `json:"accepted"`
				}
				if err = json.Unmarshal([]byte(*decoded.ResultJSON), &cached); err != nil || cached.InvoiceID != "ž-1042" || !cached.Accepted {
					t.Fatalf("cached result: %+v %v", cached, err)
				}
				if err = store.Release(ctx, key, claim.Handle); err != nil {
					t.Fatal(err)
				}
				again, failure := store.Claim(ctx, key, 42)
				if failure != nil || again.Won {
					t.Fatalf("settled release deleted record: %+v %v", again, failure)
				}
			case "release":
				if err = store.Release(ctx, key, claim.Handle); err != nil {
					t.Fatal(err)
				}
				next, failure := store.Claim(ctx, key, 42)
				if failure != nil || !next.Won {
					t.Fatalf("next: %+v %v", next, failure)
				}
			case "stale_complete", "stale_release":
				server.FastForward(2 * time.Minute)
				next, failure := store.Claim(ctx, key, 42)
				if failure != nil || !next.Won {
					t.Fatalf("replacement: %+v %v", next, failure)
				}
				if name == "stale_complete" {
					if err = store.Complete(ctx, key, claim.Handle, record); !errors.Is(err, edax.ErrStore) {
						t.Fatalf("stale complete: %v", err)
					}
				} else {
					if err = store.Release(ctx, key, claim.Handle); err != nil {
						t.Fatal(err)
					}
				}
				held, failure := store.Claim(ctx, key, 42)
				if failure != nil || held.Won {
					t.Fatalf("replacement removed: %+v %v", held, failure)
				}
				if err = store.Complete(ctx, key, next.Handle, record); err != nil {
					t.Fatal(err)
				}
			case "forget_pending":
				if err = store.Forget(ctx, key); !errors.Is(err, edax.ErrInProgress) {
					t.Fatalf("forget pending: %v", err)
				}
			case "forget_settled":
				if err = store.Complete(ctx, key, claim.Handle, record); err != nil {
					t.Fatal(err)
				}
				if err = store.Forget(ctx, key); err != nil {
					t.Fatal(err)
				}
				next, failure := store.Claim(ctx, key, 42)
				if failure != nil || !next.Won {
					t.Fatalf("forgotten: %+v %v", next, failure)
				}
			case "forget_absent":
				if err = store.Forget(ctx, "invoice:missing"); err != nil {
					t.Fatal(err)
				}
			case "corrupt_record":
				rkey, failure := kernel.RedisLedgerKey(store.config.Prefix, key)
				if failure != nil {
					t.Fatal(failure)
				}
				if err = client.Set(ctx, string(rkey), "broken ledger value", time.Hour).Err(); err != nil {
					t.Fatal(err)
				}
				prior, failure := store.Claim(ctx, key, 42)
				if failure != nil || prior.Prior == nil {
					t.Fatalf("corrupt prior: %+v %v", prior, failure)
				}
				decoded, failure := kernel.DecodeIdempotencyRecord(*prior.Prior)
				if failure != nil || decoded.ErrorCode == nil || *decoded.ErrorCode != "codec" {
					t.Fatalf("corrupt decode: %+v %v", decoded, failure)
				}
				if err = store.Forget(ctx, key); err != nil {
					t.Fatal(err)
				}
			case "claim_expiry":
				server.FastForward(2 * time.Minute)
				extended, failure := store.Extend(ctx, key, claim.Handle)
				if failure != nil || extended {
					t.Fatalf("expired lease: %v %v", extended, failure)
				}
			case "wrong_type":
				rkey, failure := kernel.RedisLedgerKey(store.config.Prefix, "wrong")
				if failure != nil {
					t.Fatal(failure)
				}
				if err = client.RPush(ctx, string(rkey), "invoice").Err(); err != nil {
					t.Fatal(err)
				}
				_, err = store.Claim(ctx, "wrong", 42)
				if !edax.IsTerminal(err) {
					t.Fatalf("WRONGTYPE must be terminal: %v", err)
				}
			}
		})
	}
}

type controlledConn struct {
	Conn
	eval func(context.Context, string, [][]byte, ...any) (any, error)
}

func (c controlledConn) Eval(ctx context.Context, s string, k [][]byte, args ...any) (any, error) {
	return c.eval(ctx, s, k, args...)
}
func TestDeadlineAndHeartbeat(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		base, _, _ := ledger(t)
		conn := controlledConn{Conn: base.conn, eval: func(ctx context.Context, _ string, _ [][]byte, _ ...any) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		store, err := New(conn, Config{Prefix: "deadline:", OpTimeout: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		_, err = store.Claim(context.Background(), "invoice:1042", 42)
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, edax.ErrStore) {
			t.Fatalf("deadline taxonomy: %v", err)
		}
	})
	for _, name := range []string{"lease_lost", "close_in_lease_lost", "terminal_failure", "transient_then_lost", "close"} {
		t.Run(name, func(t *testing.T) {
			base, _, _ := ledger(t)
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil)).With("service", "billing")
			lost := make(chan error, 1)
			calls := make(chan struct{}, 10)
			count := 0
			conn := controlledConn{Conn: base.conn, eval: func(ctx context.Context, s string, k [][]byte, args ...any) (any, error) {
				if s == base.scripts.Extend {
					count++
					calls <- struct{}{}
					switch name {
					case "terminal_failure":
						return nil, ReplyError("WRONGTYPE invalid ledger")
					case "transient_then_lost":
						if count == 1 {
							return nil, errors.New("connection interrupted")
						}
					case "close":
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return int64(0), nil
				}
				return base.conn.Eval(ctx, s, k, args...)
			}}
			var store *Store
			leaseLost := func(_ string, err error) {
				if name == "close_in_lease_lost" {
					// Shutting down on a lost lease must not wait for this worker.
					if closed := store.Close(); closed != nil {
						err = closed
					}
				}
				lost <- err
			}
			store, err := New(conn, Config{Logger: logger, Prefix: "heartbeat:", ClaimTTL: 300 * time.Millisecond, OpTimeout: 30 * time.Millisecond, Heartbeat: true, LeaseLost: leaseLost})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			claim, err := store.Claim(context.Background(), "invoice:1042", 42)
			if err != nil || !claim.Won {
				t.Fatalf("claim: %+v %v", claim, err)
			}
			if name == "close" {
				select {
				case <-calls:
				case <-time.After(time.Second):
					t.Fatal("renewal never called")
				}
				if err = store.Close(); err != nil {
					t.Fatal(err)
				}
				if logs.Len() != 0 {
					t.Fatalf("close logged lease loss: %s", logs.String())
				}
				select {
				case err = <-lost:
					t.Fatalf("close reported lease loss: %v", err)
				default:
				}
				return
			}
			select {
			case err = <-lost:
				if name == "terminal_failure" && !edax.IsTerminal(err) {
					t.Fatalf("terminal failure lost: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("no lease lost notification")
			}
			if name == "transient_then_lost" && len(calls) != 2 {
				t.Fatalf("retry count %d", len(calls))
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			output := logs.String()
			if !strings.Contains(output, "service=billing") || !strings.Contains(output, "claim lease lost") {
				t.Fatalf("injected logger missed loss: %s", output)
			}
			if name == "transient_then_lost" && !strings.Contains(output, "claim renewal failed") {
				t.Fatalf("injected logger missed transient failure: %s", output)
			}
			if strings.Contains(output, string(claim.Handle)) {
				t.Fatal("log leaked claim handle")
			}
		})
	}
}
func TestHeartbeatValidatesAppliedDeadline(t *testing.T) {
	for _, test := range []struct {
		name              string
		claimTTL, timeout time.Duration
		applied           time.Duration
		refused           bool
	}{
		{name: "whole_ms_over_third", claimTTL: 4 * time.Millisecond, timeout: 2 * time.Millisecond, refused: true},
		{name: "fraction_rounds_over_third", claimTTL: 4 * time.Millisecond, timeout: 1900 * time.Microsecond, refused: true},
		{name: "fraction_over_third_at_scale", claimTTL: 3 * time.Second, timeout: 1000900 * time.Microsecond, refused: true},
		{name: "fraction_rounds_up", claimTTL: 10 * time.Millisecond, timeout: 2100 * time.Microsecond, applied: 3 * time.Millisecond},
		{name: "whole_ms_kept", claimTTL: 10 * time.Millisecond, timeout: 3 * time.Millisecond, applied: 3 * time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := New(controlledConn{}, Config{Prefix: "heartbeat:", ClaimTTL: test.claimTTL, OpTimeout: test.timeout, Heartbeat: true})
			if test.refused {
				if !errors.Is(err, edax.ErrBadHandle) {
					t.Fatalf("accepted claimTTL=%v timeout=%v: %v", test.claimTTL, test.timeout, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if store.config.OpTimeout != test.applied {
				t.Fatalf("applied deadline %v want %v", store.config.OpTimeout, test.applied)
			}
		})
	}
}
func TestReplyClassification(t *testing.T) {
	for _, test := range []struct {
		reply    string
		terminal bool
	}{{"WRONGTYPE expected string", true}, {"NOAUTH authentication required", true}, {"READONLY replica", false}, {"LOADING dataset", false}} {
		t.Run(strings.Fields(test.reply)[0], func(t *testing.T) {
			if actual := edax.IsTerminal(ReplyError(test.reply)); actual != test.terminal {
				t.Fatalf("terminal=%v", actual)
			}
		})
	}
}

func TestCallErrorIdentityAndCause(t *testing.T) {
	for _, test := range []struct {
		name     string
		cause    error
		sentinel error
	}{
		{"network", errors.New("connection interrupted"), edax.ErrStore},
		{"deadline", context.DeadlineExceeded, edax.ErrStore},
		{"sentinel", fmt.Errorf("ledger setup: %w", edax.ErrBadHandle), edax.ErrBadHandle},
		{"typed", &edax.Error{Code: edax.CodeCodec, Message: "malformed ledger reply"}, edax.ErrCodec},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := New(controlledConn{eval: func(context.Context, string, [][]byte, ...any) (any, error) { return nil, test.cause }}, Config{Prefix: "billing:test:idem:"})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			_, first := store.Claim(context.Background(), "invoice:1042", 42)
			_, second := store.Claim(context.Background(), "invoice:1042", 42)
			var one, two *edax.Error
			if !errors.As(first, &one) || !errors.As(second, &two) {
				t.Fatalf("missing typed error: %v", first)
			}
			if one == two || one == test.sentinel || !strings.Contains(one.Message, "invoice:1042") {
				t.Fatalf("shared or incomplete error: %+v", one)
			}
			if !errors.Is(first, test.cause) || !errors.Is(first, test.sentinel) {
				t.Fatalf("cause or identity lost: %v", first)
			}
			original := two.Message
			one.Message = "changed by caller"
			if two.Message != original {
				t.Fatal("mutation crossed error instances")
			}
		})
	}
}
