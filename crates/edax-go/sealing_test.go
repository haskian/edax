package edax_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/haskian/edax/crates/edax-go"
	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

func TestKeyReservationFormattingRedactsSecret(t *testing.T) {
	reservation := edax.KeyReservation{KeyID: "generation-17", Key: []byte("private-key-secret-never-format")}
	encoded, err := json.Marshal(reservation)
	if err != nil {
		t.Fatal(err)
	}
	for name, output := range map[string]string{
		"string": fmt.Sprint(reservation),
		"fields": fmt.Sprintf("%+v", reservation),
		"go":     fmt.Sprintf("%#v", reservation),
		"json":   string(encoded),
	} {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(output, "generation-17") || !strings.Contains(output, "[REDACTED]") || strings.Contains(output, "private-key") {
				t.Fatalf("key reservation was not redacted: %s", output)
			}
		})
	}
}

func sealingDraft() edax.EventDraft {
	return edax.EventDraft{
		ID: "event-1", AggregateID: "customer-42", Tenant: edax.Ptr("tenant-7"), Subject: edax.Ptr("customer-42"),
		Sequence: edax.Ptr(uint64(1)), Name: "AddressChanged", Version: 1,
		Data:          []byte(`{"address":"17 Example Lane","customer_id":"customer-42"}`),
		CorrelationID: "flow-1", CausationID: "command-1", Metadata: map[string]string{"source": "customer-api"},
	}
}

func sealingCommand() edax.CommandMessage {
	return edax.CommandMessage{
		ID: "command-1", AggregateID: edax.Ptr("customer-42"), Tenant: edax.Ptr("tenant-7"), Subject: edax.Ptr("customer-42"),
		IdempotencyKey: edax.Ptr("request-17"), Name: "ChangeAddress", Version: 1,
		Data:          []byte(`{"address":"17 Example Lane","customer_id":"customer-42"}`),
		CorrelationID: "flow-1", CausationID: "flow-1", IssuedAt: 1_700_000_000_000_000_001,
	}
}

func TestSealingRoundTripsBothCiphersWithoutChangingRawEnvelope(t *testing.T) {
	ctx := context.Background()
	for _, cipher := range []string{"aes-256-gcm", "xchacha20-poly1305"} {
		t.Run(cipher, func(t *testing.T) {
			keys := edax.NewMemoryKeys()
			defer keys.Close()
			sealing, err := edax.NewSealing(keys, "production-eu", cipher)
			if err != nil {
				t.Fatal(err)
			}
			draft := sealingDraft()
			sealed, err := sealing.SealEvent(ctx, draft)
			if err != nil {
				t.Fatal(err)
			}
			if draft.Encryption != nil || bytes.Equal(sealed.Data, draft.Data) {
				t.Fatal("sealing changed input or retained plaintext")
			}
			event, err := kernel.RecordEvent(*sealed, 1_700_000_000_000_000_002)
			if err != nil {
				t.Fatal(err)
			}
			for _, format := range []string{"json", "msgpack"} {
				wire, err := edax.EncodeEvent(format, *event)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(wire, []byte("17 Example Lane")) {
					t.Fatal("wire retained address")
				}
				decoded, err := edax.DecodeEvent(format, wire)
				if err != nil {
					t.Fatal(err)
				}
				data, err := sealing.OpenEvent(ctx, *decoded)
				if err != nil || !bytes.Equal(data, draft.Data) {
					t.Fatalf("roundtrip %s: %s %v", format, data, err)
				}
				if !bytes.Equal(decoded.Data, sealed.Data) {
					t.Fatal("opening changed ciphertext")
				}
			}
			command := sealingCommand()
			sealedCommand, err := sealing.SealCommand(ctx, command)
			if err != nil {
				t.Fatal(err)
			}
			data, err := sealing.OpenCommand(ctx, *sealedCommand)
			if err != nil || !bytes.Equal(data, command.Data) {
				t.Fatalf("command: %v", err)
			}
			sealedCommand.Subject = edax.Ptr("customer-43")
			if _, err := sealing.OpenCommand(ctx, *sealedCommand); err == nil {
				t.Fatal("owner substitution accepted")
			}
			state := edax.StateContext{Kind: "snapshot", Identity: "customer-42", TypeName: "Customer", Version: 1, Tenant: draft.Tenant, Subject: draft.Subject}
			value, err := sealing.SealState(ctx, state, draft.Data)
			if err != nil {
				t.Fatal(err)
			}
			data, err = sealing.OpenState(ctx, state, value.Encryption, value.Data)
			if err != nil || !bytes.Equal(data, draft.Data) {
				t.Fatalf("state: %v", err)
			}
			state.Version++
			if _, err := sealing.OpenState(ctx, state, value.Encryption, value.Data); err == nil {
				t.Fatal("state version substitution accepted")
			}
		})
	}
}

func TestSealingPlaintextRequiresExplicitLegacyPolicy(t *testing.T) {
	ctx := context.Background()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	sealing, err := edax.NewSealing(keys, "production-eu", "")
	if err != nil {
		t.Fatal(err)
	}
	event, err := kernel.RecordEvent(sealingDraft(), 42)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sealing.OpenEvent(ctx, *event); err == nil {
		t.Fatal("strict reader accepted plaintext")
	}
	if _, err := sealing.OpenCommand(ctx, sealingCommand()); err == nil {
		t.Fatal("strict reader accepted plaintext command")
	}
	state := edax.StateContext{Kind: "snapshot", Identity: "customer-42", Subject: event.Subject}
	if _, err := sealing.OpenState(ctx, state, nil, event.Data); err == nil {
		t.Fatal("strict reader accepted plaintext state")
	}
	sealing.AllowPlaintext = true
	opened, err := sealing.OpenEvent(ctx, *event)
	if err != nil || !bytes.Equal(opened, event.Data) {
		t.Fatalf("legacy event: %v", err)
	}
	opened[0] ^= 1
	if bytes.Equal(opened, event.Data) {
		t.Fatal("plaintext return aliases envelope data")
	}
	if _, err := sealing.OpenCommand(ctx, sealingCommand()); err != nil {
		t.Fatal(err)
	}
	if _, err := sealing.OpenState(ctx, state, nil, event.Data); err != nil {
		t.Fatal(err)
	}
}

func TestSealingOnlyConfirmedDestructionSkipsDelivery(t *testing.T) {
	ctx := context.Background()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	sealing, err := edax.NewSealing(keys, "production-eu", "")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealing.SealEvent(ctx, sealingDraft())
	if err != nil {
		t.Fatal(err)
	}
	event, err := kernel.RecordEvent(*sealed, 42)
	if err != nil {
		t.Fatal(err)
	}
	missing := *event
	wrongInfo := *event.Encryption
	wrongInfo.KeyID = "unknown-key"
	missing.Encryption = &wrongInfo
	if _, skip, err := sealing.OpenEventDelivery(ctx, missing); err == nil || skip {
		t.Fatalf("unknown key skipped: skip=%v err=%v", skip, err)
	}
	other, err := edax.NewSealing(keys, "production-us", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, skip, err := other.OpenEventDelivery(ctx, *event); err == nil || skip {
		t.Fatalf("wrong namespace skipped: skip=%v err=%v", skip, err)
	}
	scope, err := sealing.Scope(event.Tenant, event.Subject)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Revoke(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, skip, err := sealing.OpenEventDelivery(ctx, *event); err == nil || skip {
		t.Fatalf("revoking key skipped: skip=%v err=%v", skip, err)
	}
	if _, err := keys.Destroy(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, skip, err := sealing.OpenEventDelivery(ctx, *event); err != nil || !skip {
		t.Fatalf("destroyed key: skip=%v err=%v", skip, err)
	}
	if _, err := sealing.OpenEvent(ctx, *event); err == nil {
		t.Fatal("direct read swallowed destruction")
	}
	if _, err := keys.Reserve(ctx, scope, "xchacha20-poly1305"); err == nil {
		t.Fatal("destroyed scope recreated")
	}
}

func TestSealingLegacyRestoreRespectsPermanentRevocation(t *testing.T) {
	for _, test := range []struct {
		name         string
		subject      *string
		eraseSubject *string
	}{
		{name: "subject", subject: edax.Ptr("customer-42"), eraseSubject: edax.Ptr("customer-42")},
		{name: "tenant-unowned"},
		{name: "tenant-unknown-child", subject: edax.Ptr("restored-child")},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			keys := edax.NewMemoryKeys()
			defer keys.Close()
			sealing, err := edax.NewSealing(keys, "production-eu", "")
			if err != nil {
				t.Fatal(err)
			}
			sealing.AllowPlaintext = true
			draft := sealingDraft()
			draft.Subject = test.subject
			event, err := kernel.RecordEvent(draft, 42)
			if err != nil {
				t.Fatal(err)
			}
			command := sealingCommand()
			command.Subject = test.subject
			state := edax.StateContext{Kind: "snapshot", Identity: "customer-42", TypeName: "Customer", Version: 1, Tenant: event.Tenant, Subject: test.subject}
			readScope, err := sealing.Scope(event.Tenant, test.subject)
			if err != nil {
				t.Fatal(err)
			}
			eraseScope, err := sealing.Scope(event.Tenant, test.eraseSubject)
			if err != nil {
				t.Fatal(err)
			}
			reads := map[string]func() ([]byte, error){
				"event":   func() ([]byte, error) { return sealing.OpenEvent(ctx, *event) },
				"command": func() ([]byte, error) { return sealing.OpenCommand(ctx, command) },
				"state":   func() ([]byte, error) { return sealing.OpenState(ctx, state, nil, event.Data) },
			}
			deliveries := map[string]func() ([]byte, bool, error){
				"event":   func() ([]byte, bool, error) { return sealing.OpenEventDelivery(ctx, *event) },
				"command": func() ([]byte, bool, error) { return sealing.OpenCommandDelivery(ctx, command) },
			}
			for name, read := range reads {
				data, err := read()
				if err != nil || !bytes.Equal(data, event.Data) {
					t.Fatalf("legacy %s: %q %v", name, data, err)
				}
			}
			for name, deliver := range deliveries {
				data, skip, err := deliver()
				if err != nil || skip || !bytes.Equal(data, event.Data) {
					t.Fatalf("legacy %s delivery: skip=%v err=%v", name, skip, err)
				}
			}
			for _, scope := range []edax.KeyScope{readScope, eraseScope} {
				if status, err := keys.Status(ctx, scope); err != nil || status != "unknown" {
					t.Fatalf("legacy read created keys: %s %v", status, err)
				}
			}
			if _, err := keys.Revoke(ctx, eraseScope); err != nil {
				t.Fatal(err)
			}
			for name, read := range reads {
				if data, err := read(); !errors.Is(err, edax.ErrKeyRevoking) || data != nil {
					t.Fatalf("revoking %s restored plaintext: %q %v", name, data, err)
				}
			}
			for name, deliver := range deliveries {
				if data, skip, err := deliver(); !errors.Is(err, edax.ErrKeyRevoking) || skip || data != nil {
					t.Fatalf("revoking %s delivery: skip=%v err=%v", name, skip, err)
				}
			}
			if _, err := keys.Destroy(ctx, eraseScope); err != nil {
				t.Fatal(err)
			}
			for name, read := range reads {
				if data, err := read(); !errors.Is(err, edax.ErrShredded) || data != nil {
					t.Fatalf("destroyed %s restored plaintext: %q %v", name, data, err)
				}
			}
			for name, deliver := range deliveries {
				if data, skip, err := deliver(); err != nil || !skip || data != nil {
					t.Fatalf("destroyed %s delivery: skip=%v err=%v", name, skip, err)
				}
			}
			sealing.AllowPlaintext = false
			for name, deliver := range deliveries {
				if _, skip, err := deliver(); !errors.Is(err, edax.ErrCodec) || skip {
					t.Fatalf("strict policy bypassed for %s: skip=%v err=%v", name, skip, err)
				}
			}
		})
	}
}

func TestMemoryKeyGuardFencesRevocationAndSupportsCancelledWaits(t *testing.T) {
	ctx := context.Background()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	scope := edax.KeyScope{KeyNamespace: "production-eu", Tenant: edax.Ptr("tenant-7"), Subject: edax.Ptr("customer-42")}
	release, err := keys.Hold(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := keys.Reserve(ctx, scope, "xchacha20-poly1305")
	if err != nil {
		release()
		t.Fatal(err)
	}
	clear(reserved.Key)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := keys.Hold(cancelled, scope); !errors.Is(err, context.Canceled) {
		release()
		t.Fatalf("cancelled guard: %v", err)
	}
	started, finished := make(chan struct{}), make(chan error, 1)
	go func() { close(started); _, err := keys.Revoke(ctx, scope); finished <- err }()
	<-started
	select {
	case err := <-finished:
		release()
		t.Fatalf("revoked while write held guard: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("revocation remained blocked after commit")
	}
	if release, err := keys.Hold(ctx, scope); err == nil {
		release()
		t.Fatal("revocation admitted another write")
	}
}

func TestSealingGuardReentersWithinTransactionAndExpiresOnRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	first, err := edax.NewSealing(keys, "production-eu", "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := edax.NewSealing(keys, "production-eu", "")
	if err != nil {
		t.Fatal(err)
	}
	scope, err := first.Scope(edax.Ptr("tenant-7"), edax.Ptr("customer-42"))
	if err != nil {
		t.Fatal(err)
	}
	guarded, release, err := first.Guard(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, nestedRelease, err := second.Guard(guarded, scope)
	if err != nil {
		t.Fatalf("same provider/scope deadlocked: %v", err)
	}
	nestedRelease()
	childScope := scope
	childScope.Subject = edax.Ptr("customer-43")
	_, childRelease, err := second.Guard(guarded, childScope)
	if err != nil {
		t.Fatalf("global memory guard deadlocked across scopes: %v", err)
	}
	childRelease()
	if _, err := first.SealEvent(guarded, sealingDraft()); err != nil {
		t.Fatal(err)
	}
	release()
	if _, err := keys.Revoke(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, release, err := first.Guard(guarded, scope); err == nil {
		release()
		t.Fatal("stale context bypassed revocation")
	}
}

func TestSealingNestedLeaseKeepsFenceAfterOuterRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	sealing, err := edax.NewSealing(keys, "production-eu", "")
	if err != nil {
		t.Fatal(err)
	}
	scope, err := sealing.Scope(edax.Ptr("tenant-7"), edax.Ptr("customer-42"))
	if err != nil {
		t.Fatal(err)
	}
	guarded, outer, err := sealing.Guard(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer outer()
	_, inner, err := sealing.Guard(guarded, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer inner()
	outer()
	started, finished := make(chan struct{}), make(chan error, 1)
	go func() { close(started); _, err := keys.Revoke(ctx, scope); finished <- err }()
	<-started
	select {
	case err := <-finished:
		t.Fatalf("outer release discarded nested transaction lease: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	inner()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("nested lease did not release")
	}
}

func TestSealingRejectsRevocationFromGuardedOperation(t *testing.T) {
	for _, operation := range []string{"revoke", "destroy"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			keys := edax.NewMemoryKeys()
			defer keys.Close()
			sealing, err := edax.NewSealing(keys, "production-eu", "")
			if err != nil {
				t.Fatal(err)
			}
			scope, err := sealing.Scope(edax.Ptr("tenant-7"), edax.Ptr("customer-42"))
			if err != nil {
				t.Fatal(err)
			}
			guarded, release, err := sealing.Guard(ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if operation == "revoke" {
				_, err = keys.Revoke(guarded, scope)
			} else {
				_, err = keys.Destroy(guarded, scope)
			}
			if !errors.Is(err, edax.ErrBadHandle) {
				t.Fatalf("guarded %s must refuse immediately, got %v", operation, err)
			}
		})
	}
}

func TestSealingSequentialCommandsShareTransactionFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	sealing, err := edax.NewSealing(keys, "production-eu", "")
	if err != nil {
		t.Fatal(err)
	}
	bus := edax.NewSealedCommandBus(sealing)
	calls := 0
	if err := bus.Register("ChangeAddress", func(ctx context.Context, message edax.CommandMessage) (any, error) {
		calls++
		if message.ID == "outer" {
			nested := sealingCommand()
			nested.ID = "nested"
			return bus.Dispatch(ctx, nested)
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	tx := new(cleanupTx)
	owner := &cleanupOwner{tx}
	var saved context.Context
	err = edax.WithinTransaction(ctx, owner, func(txctx context.Context) error {
		saved = txctx
		for _, id := range []string{"outer", "sequential"} {
			command := sealingCommand()
			command.ID = id
			if _, err := bus.Dispatch(txctx, command); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || calls != 3 || tx.commits != 1 {
		t.Fatalf("sequential commands: err=%v calls=%d commits=%d", err, calls, tx.commits)
	}
	scope, err := sealing.Scope(edax.Ptr("tenant-7"), edax.Ptr("customer-42"))
	if err != nil {
		t.Fatal(err)
	}
	if _, release, err := sealing.Guard(saved, scope); !errors.Is(err, edax.ErrBadHandle) {
		if release != nil {
			release()
		}
		t.Fatalf("settled transaction context admitted a guard: %v", err)
	}
	if _, err := keys.Revoke(ctx, scope); err != nil {
		t.Fatalf("transaction retained fence after commit: %v", err)
	}
}

type blockingGuardKeys struct {
	*edax.MemoryKeys
	entered chan struct{}
	proceed chan struct{}
}

func TestSealingRejectsSettledInnerTransactionContext(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed", true: "rolled_back"}[rollback], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			keys := edax.NewMemoryKeys()
			defer keys.Close()
			sealing, err := edax.NewSealing(keys, "production-eu", "")
			if err != nil {
				t.Fatal(err)
			}
			scope, err := sealing.Scope(edax.Ptr("tenant-7"), edax.Ptr("customer-42"))
			if err != nil {
				t.Fatal(err)
			}
			outer := &cleanupOwner{new(cleanupTx)}
			inner := &cleanupOwner{new(cleanupTx)}
			err = edax.WithinTransaction(ctx, outer, func(outerCtx context.Context) error {
				var saved context.Context
				innerErr := edax.WithinTransaction(outerCtx, inner, func(innerCtx context.Context) error {
					saved = innerCtx
					if rollback {
						return edax.ErrStore
					}
					return nil
				})
				if rollback && !errors.Is(innerErr, edax.ErrStore) || !rollback && innerErr != nil {
					t.Fatalf("inner transaction: %v", innerErr)
				}
				_, release, guardErr := sealing.Guard(saved, scope)
				if release != nil {
					release()
				}
				if !errors.Is(guardErr, edax.ErrBadHandle) {
					t.Errorf("settled inner context admitted a guard: %v", guardErr)
				}
				if edax.HoldTransactionResource(saved, outer, func() {}) {
					t.Error("settled inner context retained a resource on its active parent")
				}
				called := false
				newErr := edax.WithinTransaction(saved, &cleanupOwner{new(cleanupTx)}, func(context.Context) error { called = true; return nil })
				if !errors.Is(newErr, edax.ErrBadHandle) || called {
					t.Errorf("settled inner context started another transaction: %v called=%v", newErr, called)
				}
				_, release, guardErr = sealing.Guard(outerCtx, scope)
				if guardErr != nil {
					return guardErr
				}
				if !edax.HoldTransactionResource(outerCtx, outer, release) {
					release()
					t.Error("active parent refused its guard")
				}
				return nil
			})
			if err != nil || outer.tx.commits != 1 {
				t.Fatalf("outer transaction: %v commits=%d", err, outer.tx.commits)
			}
			if _, err := keys.Revoke(ctx, scope); err != nil {
				t.Fatalf("parent retained its guard after settlement: %v", err)
			}
		})
	}
}

type decoratedMemoryKeys struct{ *edax.MemoryKeys }

func TestSealingDecoratedMemoryKeysPreserveFenceIdentity(t *testing.T) {
	for _, operation := range []string{"same_scope", "other_scope", "revoke", "destroy"} {
		t.Run(operation, func(t *testing.T) {
			memoryKeys := edax.NewMemoryKeys()
			defer memoryKeys.Close()
			keys := &decoratedMemoryKeys{memoryKeys}
			sealing, err := edax.NewSealing(keys, "production-eu", "")
			if err != nil {
				t.Fatal(err)
			}
			scope, err := sealing.Scope(edax.Ptr("tenant-7"), edax.Ptr("customer-42"))
			if err != nil {
				t.Fatal(err)
			}
			ctx, release, err := sealing.Guard(context.Background(), scope)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancel()
			switch operation {
			case "same_scope", "other_scope":
				second, err := edax.NewSealing(&decoratedMemoryKeys{memoryKeys}, "production-eu", "")
				if err != nil {
					t.Fatal(err)
				}
				if operation == "other_scope" {
					scope.Subject = edax.Ptr("customer-43")
				}
				_, nestedRelease, err := second.Guard(ctx, scope)
				if nestedRelease != nil {
					nestedRelease()
				}
				if err != nil {
					t.Fatalf("decorators lost the shared memory fence: %v", err)
				}
			case "revoke", "destroy":
				if operation == "revoke" {
					_, err = keys.Revoke(ctx, scope)
				} else {
					_, err = keys.Destroy(ctx, scope)
				}
				if !errors.Is(err, edax.ErrBadHandle) {
					t.Fatalf("guarded %s must refuse without waiting for its own fence: %v", operation, err)
				}
			}
		})
	}
}

func (k *blockingGuardKeys) Hold(ctx context.Context, scope edax.KeyScope) (func(), error) {
	close(k.entered)
	select {
	case <-k.proceed:
		return k.MemoryKeys.Hold(ctx, scope)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestSealingTransactionRefusesConcurrentGuardAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	keys := &blockingGuardKeys{MemoryKeys: edax.NewMemoryKeys(), entered: make(chan struct{}), proceed: make(chan struct{})}
	defer keys.Close()
	sealing, err := edax.NewSealing(keys, "production-eu", "")
	if err != nil {
		t.Fatal(err)
	}
	scope, err := sealing.Scope(edax.Ptr("tenant-7"), edax.Ptr("customer-42"))
	if err != nil {
		t.Fatal(err)
	}
	owner := &cleanupOwner{new(cleanupTx)}
	err = edax.WithinTransaction(ctx, owner, func(txctx context.Context) error {
		finished := make(chan error, 1)
		go func() {
			_, release, err := sealing.Guard(txctx, scope)
			if err == nil && !edax.HoldTransactionResource(txctx, owner, release) {
				release()
			}
			finished <- err
		}()
		<-keys.entered
		_, release, concurrentErr := sealing.Guard(txctx, scope)
		if release != nil {
			release()
		}
		close(keys.proceed)
		if err := <-finished; err != nil {
			return err
		}
		if !errors.Is(concurrentErr, edax.ErrBadHandle) {
			t.Fatalf("concurrent guard admission was not refused: %v", concurrentErr)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type unavailableCryptoKeys struct {
	*edax.MemoryKeys
	unavailable bool
	resolves    int
}

func (k *unavailableCryptoKeys) Resolve(ctx context.Context, scope edax.KeyScope, cipher, keyID string) ([]byte, error) {
	k.resolves++
	if k.unavailable {
		return nil, &edax.Error{Code: edax.CodeKeyUnavailable, Message: "provider unavailable"}
	}
	return k.MemoryKeys.Resolve(ctx, scope, cipher, keyID)
}

func TestSealingDoesNotCacheKeysOrSkipProviderOutages(t *testing.T) {
	ctx := context.Background()
	keys := &unavailableCryptoKeys{MemoryKeys: edax.NewMemoryKeys()}
	defer keys.Close()
	sealing, err := edax.NewSealing(keys, "production-eu", "")
	if err != nil {
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
	if _, err := sealing.OpenEvent(ctx, *event); err != nil {
		t.Fatal(err)
	}
	keys.unavailable = true
	if _, skip, err := sealing.OpenEventDelivery(ctx, *event); skip || kernel.CodeOf(err) == nil || *kernel.CodeOf(err) != "key_unavailable" {
		t.Fatalf("provider outage skip=%v error=%v", skip, err)
	}
	if keys.resolves != 2 {
		t.Fatalf("second read reused secret instead of provider resolution: %d", keys.resolves)
	}
}

func TestMemoryTenantRevocationFencesNewSubjectsAndHistoricalGenerations(t *testing.T) {
	ctx := context.Background()
	keys := edax.NewMemoryKeys()
	defer keys.Close()
	scope := edax.KeyScope{KeyNamespace: "production-eu", Tenant: edax.Ptr("tenant-7"), Subject: edax.Ptr("customer-42")}
	reserved, err := keys.Reserve(ctx, scope, "xchacha20-poly1305")
	if err != nil {
		t.Fatal(err)
	}
	clear(reserved.Key)
	tenant := scope
	tenant.Subject = nil
	if _, err := keys.Revoke(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	newSubject := scope
	newSubject.Subject = edax.Ptr("customer-created-after-revocation")
	if _, err := keys.Reserve(ctx, newSubject, "xchacha20-poly1305"); kernel.CodeOf(err) == nil || *kernel.CodeOf(err) != "key_revoking" {
		t.Fatalf("new subject escaped tenant revocation: %v", err)
	}
	if _, err := keys.Destroy(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := keys.Resolve(ctx, scope, "xchacha20-poly1305", reserved.KeyID); kernel.CodeOf(err) == nil || *kernel.CodeOf(err) != "shredded" {
		t.Fatalf("historical generation escaped tenant destruction: %v", err)
	}
	if _, err := keys.Fingerprint(ctx, newSubject); kernel.CodeOf(err) == nil || *kernel.CodeOf(err) != "shredded" {
		t.Fatalf("fingerprint created under erased tenant: %v", err)
	}
}

func TestSealingGoldenEnvelopesContextsAndCiphertexts(t *testing.T) {
	// The package-internal goldenRows helper is unavailable to this external SDK test package.
	raw, err := os.ReadFile("../../testdata/golden/sealing.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct {
		Kind        string              `json:"kind"`
		Cipher      string              `json:"cipher"`
		Key         string              `json:"key_hex"`
		Plaintext   string              `json:"plaintext_hex"`
		Ciphertext  string              `json:"ciphertext_hex"`
		AAD         string              `json:"aad_hex"`
		JSON        string              `json:"json_hex"`
		MessagePack string              `json:"msgpack_hex"`
		Encryption  edax.EncryptionInfo `json:"encryption"`
		Identity    string              `json:"identity"`
		TypeName    string              `json:"type_name"`
		Version     uint64              `json:"version"`
		Tenant      *string             `json:"tenant"`
		Subject     *string             `json:"subject"`
		Marks       string              `json:"marks"`
		Completed   bool                `json:"completed"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 14 {
		t.Fatalf("expected both ciphers for seven record kinds, got %d", len(rows))
	}
	for _, row := range rows {
		t.Run(row.Kind+"/"+row.Cipher, func(t *testing.T) {
			decode := func(value string) []byte {
				t.Helper()
				decoded, err := hex.DecodeString(value)
				if err != nil {
					t.Fatal(err)
				}
				return decoded
			}
			key, plaintext, ciphertext := decode(row.Key), decode(row.Plaintext), decode(row.Ciphertext)
			var aad []byte
			switch row.Kind {
			case "event":
				message, err := edax.DecodeEvent("json", decode(row.JSON))
				if err != nil {
					t.Fatal(err)
				}
				aad, err = kernel.EventEncryptionContext(*message)
				if err != nil {
					t.Fatal(err)
				}
				for format, want := range map[string]string{"json": row.JSON, "msgpack": row.MessagePack} {
					encoded, err := edax.EncodeEvent(format, *message)
					if err != nil || !bytes.Equal(encoded, decode(want)) {
						t.Fatalf("%s encoding differs: %v", format, err)
					}
					restored, err := edax.DecodeEvent(format, decode(want))
					if err != nil || !reflect.DeepEqual(message, restored) {
						t.Fatalf("%s decoding differs: %v", format, err)
					}
				}
				var draft edax.EventDraft
				if err := json.Unmarshal(decode(row.JSON), &draft); err != nil {
					t.Fatal(err)
				}
				draft.Encryption = nil
				draft.Data = plaintext
				sealed, err := kernel.SealEvent(draft, row.Encryption, key)
				if err != nil || !bytes.Equal(sealed.Data, ciphertext) {
					t.Fatalf("event ciphertext differs: %v", err)
				}
				opened, err := kernel.OpenEvent(*message, key)
				if err != nil || !bytes.Equal(opened, plaintext) {
					t.Fatalf("event plaintext differs: %v", err)
				}
			case "command":
				message, err := edax.DecodeCommand("json", decode(row.JSON))
				if err != nil {
					t.Fatal(err)
				}
				aad, err = kernel.CommandEncryptionContext(*message)
				if err != nil {
					t.Fatal(err)
				}
				for format, want := range map[string]string{"json": row.JSON, "msgpack": row.MessagePack} {
					encoded, err := edax.EncodeCommand(format, *message)
					if err != nil || !bytes.Equal(encoded, decode(want)) {
						t.Fatalf("%s encoding differs: %v", format, err)
					}
					restored, err := edax.DecodeCommand(format, decode(want))
					if err != nil || !reflect.DeepEqual(message, restored) {
						t.Fatalf("%s decoding differs: %v", format, err)
					}
				}
				draft := *message
				draft.Encryption = nil
				draft.Data = plaintext
				sealed, err := kernel.SealCommand(draft, row.Encryption, key)
				if err != nil || !bytes.Equal(sealed.Data, ciphertext) {
					t.Fatalf("command ciphertext differs: %v", err)
				}
				opened, err := kernel.OpenCommand(*message, key)
				if err != nil || !bytes.Equal(opened, plaintext) {
					t.Fatalf("command plaintext differs: %v", err)
				}
			case "snapshot", "saga", "ingestion", "projection", "intent":
				aad, err = kernel.StateEncryptionContext(row.Kind, row.Identity, row.TypeName, row.Version, row.Marks, row.Completed, row.Encryption, row.Tenant, row.Subject)
				if err != nil {
					t.Fatal(err)
				}
				if row.Kind == "ingestion" || row.Kind == "projection" || row.Kind == "intent" {
					part, marshalErr := json.Marshal(map[string]any{"kind": row.Kind, "identity": row.Identity, "version": row.Version, "tenant": row.Tenant, "subject": row.Subject})
					if marshalErr != nil {
						t.Fatal(marshalErr)
					}
					storedAAD, contextErr := kernel.StoredEncryptionContext(string(part), row.Encryption)
					if contextErr != nil || !bytes.Equal(storedAAD, aad) {
						t.Fatalf("stored part context differs: %v", contextErr)
					}
				}
			default:
				t.Fatalf("unknown golden kind %s", row.Kind)
			}
			if !bytes.Equal(aad, decode(row.AAD)) {
				t.Fatal("authenticated context differs from golden")
			}
			sealed, err := kernel.SealBytes(row.Encryption, key, aad, plaintext)
			if err != nil || !bytes.Equal(sealed, ciphertext) {
				t.Fatalf("ciphertext differs from golden: %v", err)
			}
			opened, err := kernel.OpenBytes(row.Encryption, key, aad, ciphertext)
			if err != nil || !bytes.Equal(opened, plaintext) {
				t.Fatalf("golden authentication failed: %v", err)
			}
		})
	}
}
