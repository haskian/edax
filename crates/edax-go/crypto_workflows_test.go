package edax_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/backend/memory"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

const privateCustomer = "customer-private-17-example-lane"

type observedDeliveryKeys struct {
	*edax.MemoryKeys
	resolves      int
	destroyOnHold bool
}

func (k *observedDeliveryKeys) Resolve(ctx context.Context, scope edax.KeyScope, cipher, keyID string) ([]byte, error) {
	k.resolves++
	return k.MemoryKeys.Resolve(ctx, scope, cipher, keyID)
}

func (k *observedDeliveryKeys) Hold(ctx context.Context, scope edax.KeyScope) (func(), error) {
	if k.destroyOnHold {
		if _, err := k.MemoryKeys.Destroy(ctx, scope); err != nil {
			return nil, err
		}
	}
	return k.MemoryKeys.Hold(ctx, scope)
}

func TestCryptoDeliveryOpensEachEnvelopeOnce(t *testing.T) {
	for _, path := range []string{"command", "event"} {
		t.Run(path, func(t *testing.T) {
			ctx := context.Background()
			keys := &observedDeliveryKeys{MemoryKeys: edax.NewMemoryKeys()}
			defer keys.Close()
			sealing, err := edax.NewSealing(keys, "production-eu", "")
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			if path == "command" {
				db := memory.New()
				defer db.Close()
				ledger, err := db.IdempotencyStore()
				if err != nil {
					t.Fatal(err)
				}
				defer ledger.Close()
				idempotent, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "opened-once", Tenancy: edax.TenancyPool})
				if err != nil {
					t.Fatal(err)
				}
				bus := edax.NewSealedCommandBus(sealing, idempotent)
				if err := bus.Register("ChangeAddress", func(context.Context, edax.CommandMessage) (any, error) { calls++; return "address accepted", nil }); err != nil {
					t.Fatal(err)
				}
				command, err := sealing.SealCommand(ctx, sealingCommand())
				if err != nil {
					t.Fatal(err)
				}
				value, skip, err := bus.Deliver(ctx, *command)
				if err != nil || skip || value != "address accepted" || keys.resolves != 1 || calls != 1 {
					t.Fatalf("first delivery: value=%v skip=%v err=%v resolves=%d calls=%d", value, skip, err, keys.resolves, calls)
				}
				value, skip, err = bus.Deliver(ctx, *command)
				// Replay opens the command once and its encrypted cached result once.
				if err != nil || skip || value != "address accepted" || keys.resolves != 3 || calls != 1 {
					t.Fatalf("replay: value=%v skip=%v err=%v resolves=%d calls=%d", value, skip, err, keys.resolves, calls)
				}
			} else {
				router := edax.NewSealedEventRouter(sealing)
				if err := router.Register("AddressChanged", func(context.Context, edax.EventMessage) error { calls++; return nil }); err != nil {
					t.Fatal(err)
				}
				draft, err := sealing.SealEvent(ctx, sealingDraft())
				if err != nil {
					t.Fatal(err)
				}
				event, err := kernel.RecordEvent(*draft, 42)
				if err != nil {
					t.Fatal(err)
				}
				if err := router.Dispatch(ctx, *event); err != nil || keys.resolves != 1 || calls != 1 {
					t.Fatalf("event delivery: err=%v resolves=%d calls=%d", err, keys.resolves, calls)
				}
			}
		})
	}
}

func TestCryptoMiddlewareCannotSubstituteAuthenticatedEnvelope(t *testing.T) {
	for _, path := range []string{"command", "event"} {
		for _, field := range []string{"data", "metadata", "subject", "id"} {
			t.Run(path+"/"+field, func(t *testing.T) {
				ctx := context.Background()
				sealing, _ := workflowSealing(t, "")
				calls := 0
				if path == "command" {
					mutate := func(next edax.CommandHandler) edax.CommandHandler {
						return func(ctx context.Context, raw edax.CommandMessage) (any, error) {
							switch field {
							case "data":
								raw.Data[0] ^= 1
							case "metadata":
								raw.Metadata = map[string]string{"source": "substituted"}
							case "subject":
								*raw.Subject = "another-customer"
							case "id":
								raw.ID = "another-command"
							}
							return next(ctx, raw)
						}
					}
					bus := edax.NewSealedCommandBus(sealing, mutate)
					if err := bus.Register("ChangeAddress", func(context.Context, edax.CommandMessage) (any, error) { calls++; return nil, nil }); err != nil {
						t.Fatal(err)
					}
					command, err := sealing.SealCommand(ctx, sealingCommand())
					if err != nil {
						t.Fatal(err)
					}
					before, err := edax.EncodeCommand("json", *command)
					if err != nil {
						t.Fatal(err)
					}
					_, _, err = bus.Deliver(ctx, *command)
					if !errors.Is(err, edax.ErrCodec) || calls != 0 {
						t.Fatalf("mutated command reached handler: %v calls=%d", err, calls)
					}
					after, err := edax.EncodeCommand("json", *command)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatal("middleware mutated caller's command")
					}
				} else {
					mutate := func(next edax.EventHandler) edax.EventHandler {
						return func(ctx context.Context, raw edax.EventMessage) error {
							switch field {
							case "data":
								raw.Data[0] ^= 1
							case "metadata":
								raw.Metadata["source"] = "substituted"
							case "subject":
								*raw.Subject = "another-customer"
							case "id":
								raw.ID = "another-event"
							}
							return next(ctx, raw)
						}
					}
					router := edax.NewSealedEventRouter(sealing, mutate)
					if err := router.Register("AddressChanged", func(context.Context, edax.EventMessage) error { calls++; return nil }); err != nil {
						t.Fatal(err)
					}
					draft, err := sealing.SealEvent(ctx, sealingDraft())
					if err != nil {
						t.Fatal(err)
					}
					event, err := kernel.RecordEvent(*draft, 42)
					if err != nil {
						t.Fatal(err)
					}
					before, err := edax.EncodeEvent("json", *event)
					if err != nil {
						t.Fatal(err)
					}
					err = router.Dispatch(ctx, *event)
					if !errors.Is(err, edax.ErrCodec) || calls != 0 {
						t.Fatalf("mutated event reached handler: %v calls=%d", err, calls)
					}
					after, err := edax.EncodeEvent("json", *event)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatal("middleware mutated caller's event")
					}
				}
			})
		}
	}
}

func TestCryptoOwnerlessLegacyCommandPolicy(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-idempotent", true: "protected-idempotency"}[guarded], func(t *testing.T) {
			ctx := context.Background()
			sealing, _ := workflowSealing(t, "")
			sealing.AllowPlaintext = true
			db := memory.New()
			defer db.Close()
			ledger, err := db.IdempotencyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer ledger.Close()
			var middleware []edax.Middleware
			if guarded {
				idempotent, err := edax.Idempotent(ledger, edax.IdempotentOptions{})
				if err != nil {
					t.Fatal(err)
				}
				middleware = append(middleware, idempotent)
			}
			bus := edax.NewSealedCommandBus(sealing, middleware...)
			calls := 0
			if err := bus.Register("ChangeAddress", func(context.Context, edax.CommandMessage) (any, error) { calls++; return "legacy accepted", nil }); err != nil {
				t.Fatal(err)
			}
			command := sealingCommand()
			command.Subject = nil
			value, err := bus.Execute(ctx, command)
			if guarded {
				if !errors.Is(err, edax.ErrCodec) || calls != 0 {
					t.Fatalf("ownerless protected result admitted: %v calls=%d", err, calls)
				}
			} else if err != nil || calls != 1 || value != "legacy accepted" {
				t.Fatalf("ownerless legacy refused: %v value=%v calls=%d", err, value, calls)
			}
		})
	}
}

func TestCryptoDeliverySkipsOnlyTrustedDestruction(t *testing.T) {
	for _, path := range []string{"command", "event"} {
		for _, destroy := range []bool{false, true} {
			t.Run(path+"/"+map[bool]string{false: "handler-shredded", true: "destroyed-before-hold"}[destroy], func(t *testing.T) {
				ctx := context.Background()
				keys := &observedDeliveryKeys{MemoryKeys: edax.NewMemoryKeys(), destroyOnHold: destroy}
				defer keys.Close()
				sealing, err := edax.NewSealing(keys, "production-eu", "")
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				if path == "command" {
					bus := edax.NewSealedCommandBus(sealing)
					if err := bus.Register("ChangeAddress", func(context.Context, edax.CommandMessage) (any, error) { calls++; return nil, edax.ErrShredded }); err != nil {
						t.Fatal(err)
					}
					command, err := sealing.SealCommand(ctx, sealingCommand())
					if err != nil {
						t.Fatal(err)
					}
					_, skip, err := bus.Deliver(ctx, *command)
					if destroy {
						if err != nil || !skip || calls != 0 {
							t.Fatalf("trusted destruction: skip=%v err=%v calls=%d", skip, err, calls)
						}
						if _, err := bus.Execute(ctx, *command); !errors.Is(err, edax.ErrShredded) {
							t.Fatalf("non-delivery hid destruction: %v", err)
						}
					} else if !errors.Is(err, edax.ErrShredded) || skip || calls != 1 {
						t.Fatalf("handler error hidden: skip=%v err=%v calls=%d", skip, err, calls)
					}
				} else {
					router := edax.NewSealedEventRouter(sealing)
					if err := router.Register("AddressChanged", func(context.Context, edax.EventMessage) error { calls++; return edax.ErrShredded }); err != nil {
						t.Fatal(err)
					}
					draft, err := sealing.SealEvent(ctx, sealingDraft())
					if err != nil {
						t.Fatal(err)
					}
					event, err := kernel.RecordEvent(*draft, 42)
					if err != nil {
						t.Fatal(err)
					}
					err = router.Dispatch(ctx, *event)
					if destroy {
						if err != nil || calls != 0 {
							t.Fatalf("trusted destruction: err=%v calls=%d", err, calls)
						}
					} else if !errors.Is(err, edax.ErrShredded) || calls != 1 {
						t.Fatalf("handler error hidden: err=%v calls=%d", err, calls)
					}
				}
			})
		}
	}
}

func TestCryptoLegacyLedgerAtOrdinaryIdentityFailsClosed(t *testing.T) {
	ctx := context.Background()
	sealing, _ := workflowSealing(t, "")
	sealing.AllowPlaintext = true
	db := memory.New()
	defer db.Close()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	options := edax.IdempotentOptions{Scope: "migration-api", Tenancy: edax.TenancyPool}
	idempotent, err := edax.Idempotent(ledger, options)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	handler := func(context.Context, edax.CommandMessage) (any, error) {
		calls++
		return "completed before migration", nil
	}
	legacy := edax.NewCommandBus(idempotent)
	if err := legacy.Register("ChangeAddress", handler); err != nil {
		t.Fatal(err)
	}
	command := sealingCommand()
	if _, err := legacy.Execute(ctx, command); err != nil {
		t.Fatal(err)
	}
	ordinary, err := kernel.ClaimKey(string(options.Tenancy), options.Scope, *command.IdempotencyKey, command.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := kernel.CommandHashJSON(command.Name, command.Data)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := ledger.Claim(ctx, ordinary, hash)
	if err != nil || prior.Won || prior.Prior == nil {
		t.Fatalf("legacy fixture missing at ordinary identity: %v %v", prior, err)
	}
	protected := edax.NewSealedCommandBus(sealing, idempotent)
	if err := protected.Register("ChangeAddress", handler); err != nil {
		t.Fatal(err)
	}
	if _, err := protected.Execute(ctx, command); !errors.Is(err, edax.ErrCodec) || calls != 1 {
		t.Fatalf("migration reran completed command: err=%v calls=%d", err, calls)
	}
}

func workflowSealing(t *testing.T, cipher string) (*edax.Sealing, *edax.MemoryKeys) {
	t.Helper()
	keys := edax.NewMemoryKeys()
	t.Cleanup(func() {
		if err := keys.Close(); err != nil {
			t.Error(err)
		}
	})
	sealing, err := edax.NewSealing(keys, "production-eu", cipher)
	if err != nil {
		t.Fatal(err)
	}
	return sealing, keys
}

func workflowRepository(t *testing.T, store edax.EventStore, sealing *edax.Sealing, commands edax.CommandOutbox) *edax.Repository[*invoice] {
	t.Helper()
	codec, err := edax.NewJSONCodec(map[string]func() any{"InvoiceIssued": func() any { return new(invoiceIssued) }})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := edax.NewRepository[*invoice](store, codec, invoiceCodec{}, edax.RepositoryOptions{Sealing: sealing, Commands: commands})
	if err != nil {
		t.Fatal(err)
	}
	return repository
}

func privateInvoice(t *testing.T) *invoice {
	t.Helper()
	aggregate := &invoice{Aggregate: edax.NewAggregate("invoice-private")}
	if err := aggregate.SetSubject(edax.Ptr("person-42")); err != nil {
		t.Fatal(err)
	}
	if err := edax.Apply(aggregate, &invoiceIssued{Total: 1250, CustomerID: privateCustomer}); err != nil {
		t.Fatal(err)
	}
	return aggregate
}

func TestCryptoRepositoryPersistsOnlyCiphertextAndRestoresOwner(t *testing.T) {
	for _, cipher := range []string{"aes-256-gcm", "xchacha20-poly1305"} {
		t.Run(cipher, func(t *testing.T) {
			ctx := context.Background()
			sealing, keys := workflowSealing(t, cipher)
			db := memory.New()
			defer db.Close()
			store, err := db.EventStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			commands, err := db.CommandOutbox()
			if err != nil {
				t.Fatal(err)
			}
			defer commands.Close()
			repository := workflowRepository(t, store, sealing, commands)
			aggregate := privateInvoice(t)
			command := sealingCommand()
			command.Tenant = nil
			command.Subject = aggregate.Subject()
			rows, err := repository.Save(ctx, aggregate, edax.SaveOptions{Commands: []edax.CommandMessage{command}})
			if err != nil || len(rows) != 1 {
				t.Fatalf("save: %v %v", rows, err)
			}
			snapshot, err := store.LoadSnapshot(ctx, aggregate.ID())
			if err != nil || snapshot == nil {
				t.Fatalf("snapshot: %v %v", snapshot, err)
			}
			if held, err := commands.Unpublished(ctx, 10); err != nil || len(held) != 0 {
				t.Fatalf("a command waits behind its save's event: %v %v", held, err)
			}
			outbox, err := store.Unpublished(ctx, 10)
			if err != nil || !reflect.DeepEqual(outbox, rows) {
				t.Fatalf("outbox: %v %v", outbox, err)
			}
			if err = store.MarkPublished(ctx, []string{rows[0].ID}); err != nil {
				t.Fatal(err)
			}
			queued, err := commands.Unpublished(ctx, 10)
			if err != nil || len(queued) != 1 {
				t.Fatalf("commands: %v %v", queued, err)
			}
			for name, value := range map[string]any{"events": rows, "snapshot": snapshot, "commands": queued, "outbox": outbox} {
				wire, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(wire, []byte(privateCustomer)) || bytes.Contains(wire, []byte("17 Example Lane")) {
					t.Fatalf("%s retained fixture plaintext: %s", name, wire)
				}
			}
			if rows[0].Encryption == nil || snapshot.Encryption == nil || queued[0].Encryption == nil {
				t.Fatal("missing encryption records")
			}
			loaded, err := repository.Load(ctx, aggregate.ID(), nil)
			if err != nil || loaded.CustomerID != privateCustomer || loaded.Subject() == nil || *loaded.Subject() != "person-42" {
				t.Fatalf("reload: %+v %v", loaded, err)
			}
			if err := loaded.SetSubject(edax.Ptr("person-43")); !errors.Is(err, edax.ErrOwnerMismatch) {
				t.Fatalf("ownership change: %v", err)
			}
			if _, err := workflowRepository(t, store, nil, nil).Load(ctx, aggregate.ID(), nil); !errors.Is(err, edax.ErrBadHandle) {
				t.Fatalf("unconfigured reader: %v", err)
			}
			scope, err := sealing.Scope(nil, aggregate.Subject())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = keys.Destroy(ctx, scope); err != nil {
				t.Fatal(err)
			}
			if _, err = repository.Load(ctx, aggregate.ID(), nil); !errors.Is(err, edax.ErrShredded) {
				t.Fatalf("destroyed read: %v", err)
			}
		})
	}
}

// Uses the real kernel journal but loses the commit response before or after commit.
// The SDK still controls transaction ordering and reconciliation.
type uncertainCryptoStore struct {
	db        *kernel.MemoryTransactor
	native    *kernel.MemoryEventStore
	fail      bool
	committed bool
	batches   [][]edax.EventDraft
	snapshots []edax.Snapshot
}
type uncertainCryptoTx struct {
	store *uncertainCryptoStore
	token int64
}

func newUncertainCryptoStore(t *testing.T, committed bool) *uncertainCryptoStore {
	t.Helper()
	db := kernel.GetMemoryTransactor()
	native, err := db.EventStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { native.Close(); db.Close() })
	return &uncertainCryptoStore{db: db, native: native, fail: true, committed: committed}
}
func (s *uncertainCryptoStore) UnitOfWork() edax.UnitOfWork { return s }
func (s *uncertainCryptoStore) Begin(context.Context) (edax.Tx, error) {
	token, err := s.db.Begin()
	return &uncertainCryptoTx{s, token}, err
}
func (tx *uncertainCryptoTx) Commit(context.Context) error {
	if tx.store.fail {
		tx.store.fail = false
		if tx.store.committed {
			if err := tx.store.db.Commit(tx.token); err != nil {
				return err
			}
		} else {
			if _, err := tx.store.db.Rollback(tx.token); err != nil {
				return err
			}
		}
		return edax.ErrStore
	}
	return tx.store.db.Commit(tx.token)
}
func (tx *uncertainCryptoTx) Rollback(context.Context) error {
	_, err := tx.store.db.Rollback(tx.token)
	return err
}
func (s *uncertainCryptoStore) token(ctx context.Context) int64 {
	if tx, ok := edax.CurrentTransaction(ctx, s); ok {
		return tx.(*uncertainCryptoTx).token
	}
	return kernel.NoTx()
}
func (s *uncertainCryptoStore) Load(ctx context.Context, key string, after uint64) ([]edax.EventMessage, error) {
	return s.native.Load(s.token(ctx), key, after)
}
func (s *uncertainCryptoStore) Append(ctx context.Context, key string, version uint64, drafts []edax.EventDraft) ([]edax.EventMessage, error) {
	s.batches = append(s.batches, append([]edax.EventDraft(nil), drafts...))
	return s.native.Append(s.token(ctx), key, version, drafts, 42)
}
func (s *uncertainCryptoStore) LoadSnapshot(ctx context.Context, key string) (*edax.Snapshot, error) {
	row, err := s.native.LoadSnapshot(s.token(ctx), key)
	if err != nil || row == nil {
		return nil, err
	}
	return &edax.Snapshot{AggregateID: key, TypeName: row.TypeName, Version: row.Version, Data: row.Data, Tenant: row.Tenant, Subject: row.Subject, Encryption: row.Encryption}, nil
}
func (s *uncertainCryptoStore) SaveSnapshot(ctx context.Context, row edax.Snapshot) error {
	s.snapshots = append(s.snapshots, row)
	return s.native.SaveSnapshotRecord(s.token(ctx), row.AggregateID, kernel.SnapshotResult{TypeName: row.TypeName, Version: row.Version, Data: row.Data, Tenant: row.Tenant, Subject: row.Subject, Encryption: row.Encryption})
}

func TestCryptoRepositoryReusesPreparedCiphertextAcrossUncertainCommit(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "before_commit"
		if committed {
			name = "commit_before_response"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			sealing, _ := workflowSealing(t, "")
			store := newUncertainCryptoStore(t, committed)
			repo := workflowRepository(t, store, sealing, nil)
			aggregate := privateInvoice(t)
			if _, err := repo.Save(ctx, aggregate, edax.SaveOptions{}); !errors.Is(err, edax.ErrStore) {
				t.Fatalf("injected commit failure: %v", err)
			}
			if len(aggregate.Pending()) != 1 {
				t.Fatal("uncertain save discarded pending event")
			}
			if err := aggregate.SetSubject(edax.Ptr("person-43")); err == nil {
				t.Fatal("prepared owner changed")
			}
			retained := store.batches[0][0]
			rows, err := repo.Save(ctx, aggregate, edax.SaveOptions{})
			if err != nil || len(rows) != 1 {
				t.Fatalf("retry: %v %v", rows, err)
			}
			expected, err := kernel.RecordEvent(retained, rows[0].RecordedAt)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(rows[0], *expected) {
				t.Fatal("retry changed original identity or ciphertext")
			}
			if committed && len(store.batches) != 1 {
				t.Fatal("committed batch appended twice")
			}
			if !committed && (len(store.batches) != 2 || !reflect.DeepEqual(store.batches[0], store.batches[1])) {
				t.Fatal("rolled-back retry resealed events")
			}
			if committed && len(store.snapshots) != 1 {
				t.Fatal("committed snapshot was rewritten during reconciliation")
			}
			if !committed && (len(store.snapshots) != 2 || !reflect.DeepEqual(store.snapshots[0], store.snapshots[1])) {
				t.Fatal("rolled-back retry resealed snapshot")
			}
			persisted, err := store.Load(ctx, aggregate.ID(), 0)
			if err != nil || len(persisted) != 1 {
				t.Fatalf("stored retry: %v %v", persisted, err)
			}
		})
	}
}

// Each data reservation uses another kernel provider generation. The dedicated fingerprint key
// and revocation authority stay in MemoryKeys; this verifies SDK behavior through its public port.
type rotatingCryptoKeys struct {
	*edax.MemoryKeys
	mu          sync.Mutex
	generations map[string]*edax.MemoryKeys
}

func newRotatingCryptoKeys(t *testing.T) *rotatingCryptoKeys {
	t.Helper()
	keys := &rotatingCryptoKeys{MemoryKeys: edax.NewMemoryKeys(), generations: make(map[string]*edax.MemoryKeys)}
	t.Cleanup(func() {
		for _, generation := range keys.generations {
			generation.Close()
		}
		keys.MemoryKeys.Close()
	})
	return keys
}
func (k *rotatingCryptoKeys) Reserve(ctx context.Context, scope edax.KeyScope, cipher string) (edax.KeyReservation, error) {
	admitted, err := k.MemoryKeys.Reserve(ctx, scope, cipher)
	clear(admitted.Key)
	if err != nil {
		return edax.KeyReservation{}, err
	}
	generation := edax.NewMemoryKeys()
	reserved, err := generation.Reserve(ctx, scope, cipher)
	if err != nil {
		generation.Close()
		return reserved, err
	}
	k.mu.Lock()
	k.generations[reserved.KeyID] = generation
	k.mu.Unlock()
	return reserved, nil
}
func (k *rotatingCryptoKeys) Resolve(ctx context.Context, scope edax.KeyScope, cipher, keyID string) ([]byte, error) {
	k.mu.Lock()
	generation := k.generations[keyID]
	k.mu.Unlock()
	if generation == nil {
		return nil, edax.ErrKeyMissing
	}
	secret, err := generation.Resolve(ctx, scope, cipher, keyID)
	if err != nil {
		return nil, err
	}
	status, err := k.MemoryKeys.Status(ctx, scope)
	if err == nil {
		err = kernel.CheckKeyStatus(status)
	}
	if err != nil {
		clear(secret)
		return nil, err
	}
	return secret, nil
}

func TestCryptoCommandReplayUsesFullFingerprintAcrossDataKeyRotation(t *testing.T) {
	ctx := context.Background()
	keys := newRotatingCryptoKeys(t)
	sealing, err := edax.NewSealing(keys, "production-eu", "aes-256-gcm")
	if err != nil {
		t.Fatal(err)
	}
	db := memory.New()
	defer db.Close()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	idempotent, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "private-api", Tenancy: edax.TenancyPool})
	if err != nil {
		t.Fatal(err)
	}
	var raws []edax.CommandMessage
	capture := func(next edax.CommandHandler) edax.CommandHandler {
		return func(ctx context.Context, msg edax.CommandMessage) (any, error) {
			if msg.Encryption == nil {
				t.Error("middleware received plaintext")
			}
			raws = append(raws, msg)
			return next(ctx, msg)
		}
	}
	bus := edax.NewSealedCommandBus(sealing, capture, idempotent)
	calls := 0
	err = bus.Register("ChangeAddress", func(ctx context.Context, opened edax.CommandMessage) (any, error) {
		calls++
		raw, ok := edax.CurrentCommand(ctx)
		if !ok || raw.Encryption == nil || opened.Encryption != nil || bytes.Equal(raw.Data, opened.Data) {
			t.Error("raw/open separation lost")
		}
		return map[string]any{"address": "17 Example Lane"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	command := sealingCommand()
	first, err := bus.Dispatch(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := bus.Dispatch(ctx, command)
	if err != nil || !reflect.DeepEqual(first, second) || calls != 1 {
		t.Fatalf("replay: %v %v %d", second, err, calls)
	}
	if raws[0].Encryption.KeyID == raws[1].Encryption.KeyID || bytes.Equal(raws[0].Data, raws[1].Data) {
		t.Fatal("test did not rotate data keys")
	}
	scope, err := sealing.Scope(command.Tenant, command.Subject)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := kernel.ClaimKey("pool", "private-api", *command.IdempotencyKey, command.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	storedKey, err := kernel.ProtectedClaimKey(scope, logical)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := ledger.Claim(ctx, storedKey, 0)
	if err != nil || prior.Won || prior.Prior == nil {
		t.Fatalf("prior: %v %v", prior, err)
	}
	if strings.Contains(*prior.Prior, "17 Example Lane") {
		t.Fatal("ledger retained result plaintext")
	}
	outer, err := kernel.DecodePriorIdempotencyRecord(*prior.Prior, 0)
	if err != nil {
		t.Fatal(err)
	}
	if outer.PayloadHash != 0 || outer.State != "done" || outer.ResultJSON == nil {
		t.Fatal("invalid protected outer record")
	}
	var protected string
	if err := json.Unmarshal([]byte(*outer.ResultJSON), &protected); err != nil {
		t.Fatal(err)
	}
	var record struct {
		Fingerprint struct {
			Algorithm string  `json:"algorithm"`
			Value     []uint8 `json:"value"`
		} `json:"fingerprint"`
	}
	if err := json.Unmarshal([]byte(protected), &record); err != nil {
		t.Fatal(err)
	}
	if record.Fingerprint.Algorithm != "hmac-sha256" || len(record.Fingerprint.Value) != 32 {
		t.Fatalf("fingerprint: %+v", record.Fingerprint)
	}
	changed := command
	changed.Data = []byte(`{"address":"18 Example Lane","customer_id":"customer-42"}`)
	if _, err := bus.Dispatch(ctx, changed); !errors.Is(err, edax.ErrPayloadMismatch) {
		t.Fatalf("mismatch: %v", err)
	}
	if _, err := keys.Revoke(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Execute(ctx, raws[0]); !errors.Is(err, edax.ErrKeyRevoking) {
		t.Fatalf("revoked cached result: %v", err)
	}
}

func TestCryptoCommandCachesOnlyFailureCodeAndDoesNotSkipHandlerShredded(t *testing.T) {
	ctx := context.Background()
	sealing, keys := workflowSealing(t, "")
	db := memory.New()
	defer db.Close()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	middleware, err := edax.Idempotent(ledger, edax.IdempotentOptions{Scope: "private-api", Tenancy: edax.TenancyPool})
	if err != nil {
		t.Fatal(err)
	}
	bus := edax.NewSealedCommandBus(sealing, middleware)
	calls := 0
	if err := bus.Register("ChangeAddress", func(context.Context, edax.CommandMessage) (any, error) {
		calls++
		return nil, &edax.Error{Code: edax.CodeRejected, Message: "17 Example Lane was refused"}
	}); err != nil {
		t.Fatal(err)
	}
	command := sealingCommand()
	sealed, err := sealing.SealCommand(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Execute(ctx, *sealed); !errors.Is(err, edax.ErrRejected) {
		t.Fatal(err)
	}
	if _, err := bus.Execute(ctx, *sealed); !errors.Is(err, edax.ErrCachedFailure) || strings.Contains(err.Error(), "17 Example Lane") {
		t.Fatalf("safe replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("failure handler repeated: %d", calls)
	}
	scope, err := sealing.Scope(command.Tenant, command.Subject)
	if err != nil {
		t.Fatal(err)
	}
	logical, _ := kernel.ClaimKey("pool", "private-api", *command.IdempotencyKey, command.Tenant)
	stored, _ := kernel.ProtectedClaimKey(scope, logical)
	prior, err := ledger.Claim(ctx, stored, 0)
	if err != nil || prior.Prior == nil {
		t.Fatal(err)
	}
	if strings.Contains(*prior.Prior, "17 Example Lane") {
		t.Fatal("failure text persisted")
	}
	failing := edax.NewSealedCommandBus(sealing)
	if err := failing.Register("ChangeAddress", func(context.Context, edax.CommandMessage) (any, error) { return nil, edax.ErrShredded }); err != nil {
		t.Fatal(err)
	}
	if _, skip, err := failing.Deliver(ctx, *sealed); !errors.Is(err, edax.ErrShredded) || skip {
		t.Fatalf("handler skip=%v error=%v", skip, err)
	}
	if _, err := keys.Destroy(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, skip, err := failing.Deliver(ctx, *sealed); err != nil || !skip {
		t.Fatalf("destroyed skip=%v error=%v", skip, err)
	}
}

func TestCryptoRouterOpenedViewCannotMutateRawEnvelope(t *testing.T) {
	ctx := context.Background()
	sealing, _ := workflowSealing(t, "")
	draft, err := sealing.SealEvent(ctx, sealingDraft())
	if err != nil {
		t.Fatal(err)
	}
	event, err := kernel.RecordEvent(*draft, 42)
	if err != nil {
		t.Fatal(err)
	}
	before, err := edax.EncodeEvent("json", *event)
	if err != nil {
		t.Fatal(err)
	}
	inspect := func(next edax.EventHandler) edax.EventHandler {
		return func(ctx context.Context, raw edax.EventMessage) error {
			if raw.Encryption == nil {
				t.Error("middleware received plaintext")
			}
			err := next(ctx, raw)
			after, encodeErr := edax.EncodeEvent("json", raw)
			if encodeErr != nil || !bytes.Equal(before, after) {
				t.Errorf("handler changed guard envelope: %v", encodeErr)
			}
			return err
		}
	}
	router := edax.NewSealedEventRouter(sealing, inspect)
	if err := router.Register(event.Name, func(_ context.Context, view edax.EventMessage) error {
		if view.Encryption != nil || !bytes.Contains(view.Data, []byte("17 Example Lane")) {
			t.Error("handler did not receive plaintext")
		}
		view.Metadata["source"] = "modified"
		*view.Subject = "modified"
		view.Data[0] ^= 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := router.Dispatch(ctx, *event); err != nil {
		t.Fatal(err)
	}
	after, err := edax.EncodeEvent("json", *event)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("original envelope changed: %v", err)
	}
}

func TestCryptoAmbientTransactionHoldsFenceAfterHandlerReturns(t *testing.T) {
	for _, path := range []string{"repository", "command"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sealing, keys := workflowSealing(t, "")
			db := memory.New()
			defer db.Close()
			store, err := db.EventStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			repo := workflowRepository(t, store, sealing, nil)
			aggregate := privateInvoice(t)
			bus := edax.NewSealedCommandBus(sealing)
			if err := bus.Register("ChangeAddress", func(context.Context, edax.CommandMessage) (any, error) { return nil, nil }); err != nil {
				t.Fatal(err)
			}
			command := sealingCommand()
			command.Tenant = nil
			command.Subject = aggregate.Subject()
			scope, err := sealing.Scope(nil, aggregate.Subject())
			if err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			err = edax.WithinTransaction(ctx, db, func(txctx context.Context) error {
				if path == "repository" {
					if _, err := repo.Save(txctx, aggregate, edax.SaveOptions{}); err != nil {
						return err
					}
				} else {
					if _, err := bus.Dispatch(txctx, command); err != nil {
						return err
					}
				}
				started := make(chan struct{})
				go func() { close(started); _, err := keys.Revoke(ctx, scope); finished <- err }()
				<-started
				select {
				case err := <-finished:
					t.Errorf("cleanup overtook ambient commit: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("fence remained after transaction committed")
			}
			if path == "repository" {
				rows, err := store.Load(ctx, aggregate.ID(), 0)
				if err != nil || len(rows) != 1 {
					t.Fatalf("commit missing: %v %v", rows, err)
				}
			}
		})
	}
}

type cryptoAddressSaga struct{ calls int }

func (*cryptoAddressSaga) Name() string { return "AddressWorkflow" }
func (*cryptoAddressSaga) InstanceID(event edax.EventMessage) (string, bool) {
	return event.AggregateID, true
}
func (s *cryptoAddressSaga) Handle(_ context.Context, _ json.RawMessage, event edax.EventMessage) (edax.SagaStep, error) {
	s.calls++
	if event.Encryption != nil {
		return edax.SagaStep{}, edax.ErrCodec
	}
	if event.Name == "AddressConfirmed" {
		return edax.SagaStep{Complete: true, State: append(json.RawMessage(nil), event.Data...)}, nil
	}
	return edax.SagaStep{State: append(json.RawMessage(nil), event.Data...), Commands: []edax.Payload{{Name: "ConfirmAddress", Version: 1, Data: append([]byte(nil), event.Data...)}}}, nil
}

type tamperedCryptoSagaStore struct {
	edax.SagaStore
	completed *bool
}

func (s tamperedCryptoSagaStore) Load(ctx context.Context, name, id string) (*edax.SagaInstance, error) {
	row, err := s.SagaStore.Load(ctx, name, id)
	if row != nil && s.completed != nil {
		row.Completed = *s.completed
	}
	return row, err
}

func TestCryptoSagaPersistsEncryptedStateCommandsAndOwnedCompletion(t *testing.T) {
	ctx := context.Background()
	sealing, _ := workflowSealing(t, "")
	db := memory.New()
	defer db.Close()
	store, err := db.SagaStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	saga := &cryptoAddressSaga{}
	runner, err := edax.NewSagaRunner(saga, store, edax.SagaOptions{Sealing: sealing})
	if err != nil {
		t.Fatal(err)
	}
	sealEvent := func(draft edax.EventDraft) edax.EventMessage {
		t.Helper()
		sealed, err := sealing.SealEvent(ctx, draft)
		if err != nil {
			t.Fatal(err)
		}
		event, err := kernel.RecordEvent(*sealed, 42)
		if err != nil {
			t.Fatal(err)
		}
		return *event
	}
	raw := sealEvent(sealingDraft())
	router := edax.NewSealedEventRouter(sealing)
	if err := router.Register(raw.Name, runner.OnEvent); err != nil {
		t.Fatal(err)
	}
	if err := router.Dispatch(ctx, raw); err != nil {
		t.Fatal(err)
	}
	instance, err := store.Load(ctx, saga.Name(), raw.AggregateID)
	if err != nil || instance == nil || instance.Protected == nil {
		t.Fatalf("encrypted instance: %+v %v", instance, err)
	}
	if len(instance.State) != 0 || instance.Completed || saga.calls != 1 {
		t.Fatalf("instance application state exposed: %+v", instance)
	}
	wire, err := json.Marshal(instance)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte("17 Example Lane")) {
		t.Fatal("saga fixture plaintext persisted")
	}
	commands, err := runner.Outbox().Unpublished(ctx, 10)
	if err != nil || len(commands) != 1 || commands[0].Encryption == nil {
		t.Fatalf("protected saga outbox: %v %v", commands, err)
	}
	opened, err := sealing.OpenCommand(ctx, commands[0])
	if err != nil || !bytes.Contains(opened, []byte("17 Example Lane")) {
		t.Fatalf("saga command: %s %v", opened, err)
	}
	if err := router.Dispatch(ctx, raw); err != nil || saga.calls != 1 {
		t.Fatalf("saga replay calls=%d err=%v", saga.calls, err)
	}
	foreign := sealingDraft()
	foreign.ID = "event-foreign"
	foreign.Sequence = edax.Ptr(uint64(2))
	foreign.Subject = edax.Ptr("customer-43")
	if err := runner.OnEvent(ctx, sealEvent(foreign)); !errors.Is(err, edax.ErrOwnerMismatch) || saga.calls != 1 {
		t.Fatalf("foreign owner accepted: calls=%d err=%v", saga.calls, err)
	}
	tampered, err := edax.NewSagaRunner(saga, tamperedCryptoSagaStore{SagaStore: store, completed: edax.Ptr(true)}, edax.SagaOptions{Sealing: sealing})
	if err != nil {
		t.Fatal(err)
	}
	next := sealingDraft()
	next.ID = "event-next"
	next.Sequence = edax.Ptr(uint64(2))
	if err := tampered.OnEvent(ctx, sealEvent(next)); err == nil {
		t.Fatal("unauthenticated completed flag suppressed active saga")
	}
	completed := next
	completed.ID = "event-completed"
	completed.Name = "AddressConfirmed"
	if err := runner.OnEvent(ctx, sealEvent(completed)); err != nil {
		t.Fatal(err)
	}
	instance, err = store.Load(ctx, saga.Name(), raw.AggregateID)
	if err != nil || instance == nil || !instance.Completed || instance.Protected == nil {
		t.Fatalf("protected completion tombstone: %+v %v", instance, err)
	}
	if saga.calls != 2 {
		t.Fatalf("completion calls=%d", saga.calls)
	}
	if err := runner.OnEvent(ctx, sealEvent(completed)); err != nil || saga.calls != 2 {
		t.Fatalf("completion replay: calls=%d err=%v", saga.calls, err)
	}
	if err := runner.OnEvent(ctx, sealEvent(foreign)); !errors.Is(err, edax.ErrOwnerMismatch) {
		t.Fatalf("completed owner was not retained: %v", err)
	}
}

// Embedding only the port deliberately hides the underlying store's UnitOfWork capability.
type standaloneCryptoLedger struct{ edax.IdempotencyStore }

func TestCryptoSagaCommandFailedCompensatesOnceWithoutErrorText(t *testing.T) {
	for _, cipher := range []string{"aes-256-gcm", "xchacha20-poly1305"} {
		t.Run(cipher, func(t *testing.T) {
			ctx := context.Background()
			sealing, _ := workflowSealing(t, cipher)
			db := memory.New()
			defer db.Close()
			store, err := db.SagaStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			saga := &compensatingPurchase{}
			runner, err := edax.NewSagaRunner(saga, store, edax.SagaOptions{Sealing: sealing})
			if err != nil {
				t.Fatal(err)
			}
			draft, err := sealing.SealEvent(ctx, edax.EventDraft{
				ID: "payment-1", AggregateID: "order-1", Tenant: edax.Ptr("tenant-acme"), Subject: edax.Ptr("customer-42"),
				Sequence: edax.Ptr(uint64(1)), Name: "PaymentReceived", Version: 1,
				Data:          []byte(`{"order_id":"order-1","total":1250,"currency":"EUR"}`),
				CorrelationID: "purchase-flow-1", CausationID: "payment-command-1",
			})
			if err != nil {
				t.Fatal(err)
			}
			event, err := kernel.RecordEvent(*draft, 42)
			if err != nil {
				t.Fatal(err)
			}
			if err := runner.OnEvent(ctx, *event); err != nil {
				t.Fatal(err)
			}
			commands, err := runner.Outbox().Unpublished(ctx, 10)
			if err != nil || len(commands) != 2 {
				t.Fatalf("initial commands: %+v %v", commands, err)
			}
			failed := commands[0]
			const privateFailure = "customer-private@example.test: card declined at 17 Example Lane"
			dead := edax.DeadLetter[edax.CommandMessage]{Message: failed, Attempt: 3, Failure: &edax.Error{Code: edax.CodeRejected, Message: privateFailure}}
			if err := runner.CommandFailed(ctx, dead); err != nil {
				t.Fatal(err)
			}
			instance, err := store.Load(ctx, saga.Name(), "order-1")
			if err != nil || instance == nil || instance.Protected == nil || instance.Completed || instance.Version != 2 || len(instance.State) != 0 {
				t.Fatalf("protected compensation state: %+v %v", instance, err)
			}
			protected := instance.Protected
			if !reflect.DeepEqual(protected.Tenant, event.Tenant) || !reflect.DeepEqual(protected.Subject, event.Subject) || protected.Encryption == nil || protected.Encryption.Cipher != cipher {
				t.Fatalf("compensation state lost encryption or ownership: %+v", protected)
			}
			persisted, err := json.Marshal(instance)
			if err != nil || bytes.Contains(persisted, []byte(privateFailure)) {
				t.Fatalf("failure text persisted in state: %s %v", persisted, err)
			}
			plaintext, err := sealing.OpenState(ctx, edax.StateContext{Kind: "saga", Identity: "order-1", TypeName: saga.Name(), Version: instance.Version, Tenant: protected.Tenant, Subject: protected.Subject, Marks: instance.Folded}, protected.Encryption, protected.Data)
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				Failed []string `json:"failed"`
			}
			if err := json.Unmarshal(plaintext, &state); err != nil || !reflect.DeepEqual(state.Failed, []string{failed.ID}) || bytes.Contains(plaintext, []byte(privateFailure)) {
				t.Fatalf("compensation state: %s %v", plaintext, err)
			}
			if len(saga.failures) != 1 {
				t.Fatalf("failure handler calls: %d", len(saga.failures))
			}
			var failure struct {
				CommandID string `json:"command_id"`
				Command   string `json:"command"`
				Code      string `json:"code"`
				Error     string `json:"error"`
			}
			if err := json.Unmarshal(saga.failures[0], &failure); err != nil || failure.CommandID != failed.ID || failure.Command != failed.Name || failure.Code != "rejected" || failure.Error != "" {
				t.Fatalf("failure handler received unsanitized or incomplete context: %s %v", saga.failures[0], err)
			}
			commands, err = runner.Outbox().Unpublished(ctx, 10)
			if err != nil || len(commands) != 3 {
				t.Fatalf("compensation outbox: %+v %v", commands, err)
			}
			refunds := 0
			for _, command := range commands {
				if command.Encryption == nil || command.Encryption.Cipher != cipher || !reflect.DeepEqual(command.Tenant, event.Tenant) || !reflect.DeepEqual(command.Subject, event.Subject) {
					t.Fatalf("outbox command lost encryption or ownership: %+v", command)
				}
				wire, err := edax.EncodeCommand("json", command)
				if err != nil || bytes.Contains(wire, []byte(privateFailure)) {
					t.Fatalf("failure text persisted in outbox: %s %v", wire, err)
				}
				payload, err := sealing.OpenCommand(ctx, command)
				if err != nil || bytes.Contains(payload, []byte(privateFailure)) {
					t.Fatalf("failure text included in command payload: %s %v", payload, err)
				}
				if command.Name == "RefundOrder" {
					refunds++
					var refund struct {
						OrderID       string `json:"order_id"`
						FailedCommand string `json:"failed_command"`
					}
					if err := json.Unmarshal(payload, &refund); err != nil || refund.OrderID != "order-1" || refund.FailedCommand != failed.ID {
						t.Fatalf("wrong compensation: %s %v", payload, err)
					}
				}
			}
			if refunds != 1 {
				t.Fatalf("compensation count: %d", refunds)
			}
			if err := runner.CommandFailed(ctx, dead); err != nil {
				t.Fatal(err)
			}
			replayed, err := store.Load(ctx, saga.Name(), "order-1")
			if err != nil || !reflect.DeepEqual(replayed, instance) || saga.calls != 2 || len(saga.failures) != 1 {
				t.Fatalf("failure replay changed state or reran handler: %+v calls=%d failures=%d err=%v", replayed, saga.calls, len(saga.failures), err)
			}
			replayedCommands, err := runner.Outbox().Unpublished(ctx, 10)
			if err != nil || !reflect.DeepEqual(replayedCommands, commands) {
				t.Fatalf("failure replay changed outbox: %+v %v", replayedCommands, err)
			}
		})
	}
}

func TestCryptoProtectedLedgerPreservesStandaloneTransactionCapability(t *testing.T) {
	ctx := context.Background()
	sealing, _ := workflowSealing(t, "")
	ledgerDB := memory.New()
	defer ledgerDB.Close()
	workDB := memory.New()
	defer workDB.Close()
	ledger, err := ledgerDB.IdempotencyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	guard, err := edax.Idempotent(standaloneCryptoLedger{ledger}, edax.IdempotentOptions{Scope: "private-api", Tenancy: edax.TenancyPool, UnitOfWork: workDB})
	if err != nil {
		t.Fatal(err)
	}
	bus := edax.NewSealedCommandBus(sealing, guard)
	calls := 0
	if err := bus.Register("ChangeAddress", func(ctx context.Context, _ edax.CommandMessage) (any, error) {
		calls++
		if _, ok := edax.CurrentTransaction(ctx, workDB); !ok {
			return nil, edax.ErrBadHandle
		}
		return "accepted", nil
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := bus.Dispatch(ctx, sealingCommand())
		if err != nil || result != "accepted" {
			t.Fatalf("standalone protected ledger: %v %v", result, err)
		}
	}
	if calls != 1 {
		t.Fatalf("standalone replay ran handler %d times", calls)
	}
}

func TestCryptoRouterAuthenticatesBeforeReplayingEventGuard(t *testing.T) {
	ctx := context.Background()
	sealing, _ := workflowSealing(t, "")
	db := memory.New()
	defer db.Close()
	ledger, err := db.IdempotencyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	guard, err := edax.IdempotentByID(ledger, edax.IdempotentOptions{Scope: "private-projection", Tenancy: edax.TenancyPool})
	if err != nil {
		t.Fatal(err)
	}
	router := edax.NewSealedEventRouter(sealing, guard)
	calls := 0
	if err := router.Register("AddressChanged", func(context.Context, edax.EventMessage) error { calls++; return nil }); err != nil {
		t.Fatal(err)
	}
	draft, err := sealing.SealEvent(ctx, sealingDraft())
	if err != nil {
		t.Fatal(err)
	}
	event, err := kernel.RecordEvent(*draft, 42)
	if err != nil {
		t.Fatal(err)
	}
	if err := router.Dispatch(ctx, *event); err != nil {
		t.Fatal(err)
	}
	wire, err := edax.EncodeEvent("json", *event)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := edax.DecodeEvent("json", wire)
	if err != nil {
		t.Fatal(err)
	}
	changed.Metadata["source"] = "tampered-after-first-delivery"
	if err := router.Dispatch(ctx, *changed); !errors.Is(err, edax.ErrCodec) {
		t.Fatalf("cached event guard bypassed envelope authentication: %v", err)
	}
	if calls != 1 {
		t.Fatalf("tampered cached event ran handler: %d", calls)
	}
}

func TestCryptoCommandRetriesRetainOnePreparedEnvelope(t *testing.T) {
	sealing, _ := workflowSealing(t, "")
	retry, err := edax.Retry(2, edax.Pacing{Sleep: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	var attempts [][]byte
	capture := func(next edax.CommandHandler) edax.CommandHandler {
		return func(ctx context.Context, raw edax.CommandMessage) (any, error) {
			wire, err := edax.EncodeCommand("json", raw)
			if err != nil {
				return nil, err
			}
			attempts = append(attempts, wire)
			return next(ctx, raw)
		}
	}
	bus := edax.NewSealedCommandBus(sealing, retry, capture)
	calls := 0
	if err := bus.Register("ChangeAddress", func(context.Context, edax.CommandMessage) (any, error) {
		calls++
		if calls == 1 {
			return nil, edax.ErrStore
		}
		return "accepted", nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := bus.Dispatch(context.Background(), sealingCommand())
	if err != nil || result != "accepted" || len(attempts) != 2 {
		t.Fatalf("retry: %v %v attempts=%d", result, err, len(attempts))
	}
	if !bytes.Equal(attempts[0], attempts[1]) {
		t.Fatal("retry changed message identity, nonce or ciphertext")
	}
}
