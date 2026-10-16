package edax

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/haskian/edax/crates/edax-go/internal/kernel"
)

// KeyScope identifies a deployment, tenant, and erasure owner independently of storage tenancy.
type KeyScope = kernel.KeyScope

// KeyReservation is one provider-owned generation reserved for one host operation.
// Key must be a fresh owned byte slice: Sealing clears it after use.
// Formatting and JSON serialization redact the key.
type KeyReservation struct {
	KeyID string
	Key   []byte
}

func (r KeyReservation) String() string {
	return fmt.Sprintf("KeyReservation{KeyID:%q Key:[REDACTED]}", r.KeyID)
}

func (r KeyReservation) GoString() string { return r.String() }

func (r KeyReservation) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		KeyID string `json:"key_id"`
		Key   string `json:"key"`
	}{r.KeyID, "[REDACTED]"})
}

// SealedValue holds encrypted stored state and its public encryption information.
type SealedValue = kernel.SealedValue

// KeyProvider is the asynchronous I/O boundary for production key storage.
// Resolve never creates keys. Every returned secret slice belongs to the caller.
// Hold fences writes through commit, including tenant revocation of child scopes.
// Production implementations coordinate this guard with the destination transaction.
type KeyProvider interface {
	Reserve(context.Context, KeyScope, string) (KeyReservation, error)
	Resolve(context.Context, KeyScope, string, string) ([]byte, error)
	Fingerprint(context.Context, KeyScope) (KeyReservation, error)
	ResolveFingerprint(context.Context, KeyScope, string) ([]byte, error)
	Revoke(context.Context, KeyScope) (string, error)
	Destroy(context.Context, KeyScope) (string, error)
	Status(context.Context, KeyScope) (string, error)
	Hold(context.Context, KeyScope) (release func(), err error)
}

// MemoryKeys is a process-local reference provider. Restart loses its keys and tombstones.
// The global write guard favors simple, verifiable fencing over concurrency.
type MemoryKeys struct {
	native *kernel.MemoryKeys
	guard  chan struct{}
}

// Embedding decorators retain the identity and global scope of this fence.
func (k *MemoryKeys) memoryKeyFence() *MemoryKeys { return k }

type memoryKeyFence interface {
	memoryKeyFence() *MemoryKeys
}

// NewMemoryKeys creates a reference provider. Production deployments supply durable KeyProvider implementations.
func NewMemoryKeys() *MemoryKeys {
	keys := &MemoryKeys{native: kernel.GetMemoryKeys(), guard: make(chan struct{}, 1)}
	keys.guard <- struct{}{}
	return keys
}

// Close releases the native provider after active guarded writes finish.
func (k *MemoryKeys) Close() error {
	if k == nil || k.native == nil {
		return nil
	}
	release, err := k.acquire(context.Background())
	if err != nil {
		return err
	}
	defer release()
	return k.native.Close()
}

func (k *MemoryKeys) acquire(ctx context.Context) (func(), error) {
	if k == nil || k.native == nil || k.guard == nil {
		return nil, closedHandle("key provider")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-k.guard:
	}
	var once sync.Once
	release := func() { once.Do(func() { k.guard <- struct{}{} }) }
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func (k *MemoryKeys) ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if k == nil || k.native == nil {
		return closedHandle("key provider")
	}
	return nil
}

// Reserve atomically consumes one encryption invocation before encryption starts.
func (k *MemoryKeys) Reserve(ctx context.Context, scope KeyScope, cipher string) (KeyReservation, error) {
	if err := k.ready(ctx); err != nil {
		return KeyReservation{}, err
	}
	reserved, err := k.native.Reserve(scope, cipher)
	if err != nil {
		return KeyReservation{}, err
	}
	key, err := k.native.Resolve(scope, cipher, reserved.KeyID)
	return KeyReservation{KeyID: reserved.KeyID, Key: key}, err
}

// Resolve reads one existing key from its exact scope, cipher, and generation.
func (k *MemoryKeys) Resolve(ctx context.Context, scope KeyScope, cipher, keyID string) ([]byte, error) {
	if err := k.ready(ctx); err != nil {
		return nil, err
	}
	return k.native.Resolve(scope, cipher, keyID)
}

// Fingerprint reserves the scope's stable fingerprint key.
func (k *MemoryKeys) Fingerprint(ctx context.Context, scope KeyScope) (KeyReservation, error) {
	if err := k.ready(ctx); err != nil {
		return KeyReservation{}, err
	}
	reserved, err := k.native.Fingerprint(scope)
	if err != nil {
		return KeyReservation{}, err
	}
	key, err := k.native.ResolveFingerprint(scope, reserved.KeyID)
	return KeyReservation{KeyID: reserved.KeyID, Key: key}, err
}

// ResolveFingerprint reads only an existing fingerprint key in this exact scope.
func (k *MemoryKeys) ResolveFingerprint(ctx context.Context, scope KeyScope, keyID string) ([]byte, error) {
	if err := k.ready(ctx); err != nil {
		return nil, err
	}
	return k.native.ResolveFingerprint(scope, keyID)
}

// Revoke waits for admitted writes and permanently fences subsequent work.
func (k *MemoryKeys) Revoke(ctx context.Context, scope KeyScope) (string, error) {
	if k.guarded(ctx) {
		return "", &Error{Code: CodeBadHandle, Message: "cannot revoke keys from a guarded operation"}
	}
	release, err := k.acquire(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	return k.native.Revoke(scope)
}

// Destroy removes key generations after admitted writes finish and preserves revocation tombstones.
func (k *MemoryKeys) Destroy(ctx context.Context, scope KeyScope) (string, error) {
	if k.guarded(ctx) {
		return "", &Error{Code: CodeBadHandle, Message: "cannot destroy keys from a guarded operation"}
	}
	release, err := k.acquire(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	return k.native.Destroy(scope)
}

func (k *MemoryKeys) guarded(ctx context.Context) bool {
	for held, _ := ctx.Value(sealingGuardKey{}).(*sealingGuard); held != nil; held = held.parent {
		if held.active.Load() && sameKeyProvider(held.provider, k) {
			return true
		}
	}
	if resources, ok := ctx.Value(transactionRootResources{}).(*transactionResources); ok {
		resources.mu.Lock()
		defer resources.mu.Unlock()
		for _, held := range resources.guards {
			if held.active.Load() && sameKeyProvider(held.provider, k) {
				return true
			}
		}
	}
	return false
}

// Status reports the kernel's authoritative scope lifecycle state.
func (k *MemoryKeys) Status(ctx context.Context, scope KeyScope) (string, error) {
	if err := k.ready(ctx); err != nil {
		return "", err
	}
	return k.native.Status(scope)
}

// Hold admits a write and blocks revocation until release. Keep the guard through commit.
// Call Reserve/Resolve while holding it, but never Revoke/Destroy/Close from the guarded operation.
func (k *MemoryKeys) Hold(ctx context.Context, scope KeyScope) (func(), error) {
	release, err := k.acquire(ctx)
	if err != nil {
		return nil, err
	}
	status, err := k.Status(ctx, scope)
	if err == nil {
		err = kernel.CheckKeyWriteStatus(status)
	}
	if err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// Sealing configures optional encryption. Finish configuration before concurrent use.
// No reusable key cache is maintained. AllowPlaintext is an explicit legacy migration policy.
type Sealing struct {
	Keys           KeyProvider
	KeyNamespace   string
	Cipher         string
	AllowPlaintext bool
}

// NewSealing validates the namespace and cipher. Empty cipher selects xchacha20-poly1305.
func NewSealing(keys KeyProvider, keyNamespace, cipher string) (*Sealing, error) {
	if keys == nil {
		return nil, &Error{Code: CodeBadHandle, Message: "sealing requires a key provider"}
	}
	if cipher == "" {
		cipher = kernel.DefaultCipher()
	}
	if err := kernel.CheckKeyScope(KeyScope{KeyNamespace: keyNamespace}); err != nil {
		return nil, err
	}
	if _, err := kernel.EncryptionNonce(cipher); err != nil {
		return nil, err
	}
	return &Sealing{Keys: keys, KeyNamespace: keyNamespace, Cipher: cipher}, nil
}

// Scope validates a provider scope without allocating keys.
func (s *Sealing) Scope(tenant, subject *string) (KeyScope, error) {
	if s == nil || s.Keys == nil {
		return KeyScope{}, &Error{Code: CodeBadHandle, Message: "sealing requires a key provider"}
	}
	scope := KeyScope{KeyNamespace: s.KeyNamespace, Tenant: tenant, Subject: subject}
	return scope, kernel.CheckKeyScope(scope)
}

// CheckLegacy applies the migration policy and checks permanent revocation without creating keys.
// Historical records may have no subject; their tenant tombstone still governs access.
func (s *Sealing) CheckLegacy(ctx context.Context, tenant, subject *string) error {
	if s == nil || s.Keys == nil {
		return &Error{Code: CodeBadHandle, Message: "sealing requires a key provider"}
	}
	if err := kernel.CheckPlaintextPolicy(s.AllowPlaintext); err != nil {
		return err
	}
	scope, err := s.Scope(tenant, subject)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := s.Keys.Status(ctx, scope)
	if err != nil {
		return err
	}
	return kernel.CheckKeyWriteStatus(status)
}

type sealingGuardKey struct{}

type sealingGuard struct {
	sealing  *Sealing
	provider KeyProvider
	scope    string
	active   atomic.Bool
	refs     atomic.Int64
	release  func()
	parent   *sealingGuard
	// Only the transaction resource mutex reads or writes acquiring.
	acquiring bool
}

// Guard holds the provider's write fence until release. Pass the returned context to nested SDK calls.
// Guard contexts belong to one serialized transaction: do not share them with spawned goroutines
// or use nested work after the outer transaction releases its guard.
func (s *Sealing) Guard(ctx context.Context, scope KeyScope) (context.Context, func(), error) {
	if s == nil || s.Keys == nil {
		return ctx, nil, &Error{Code: CodeBadHandle, Message: "sealing requires a key provider"}
	}
	if err := ctx.Err(); err != nil {
		return ctx, nil, err
	}
	if err := checkTransactionResources(ctx); err != nil {
		return ctx, nil, err
	}
	encoded, err := kernel.EncodeKeyScope(scope)
	if err != nil {
		return ctx, nil, err
	}
	parent, _ := ctx.Value(sealingGuardKey{}).(*sealingGuard)
	resources, _ := ctx.Value(transactionRootResources{}).(*transactionResources)
	if resources != nil {
		resources.mu.Lock()
		defer resources.mu.Unlock()
		if err := checkTransactionResources(ctx); err != nil {
			return ctx, nil, err
		}
	}
	reenter := func(held *sealingGuard) (context.Context, func(), error) {
		if resources != nil {
			resources.mu.Unlock()
			defer resources.mu.Lock()
		}
		return s.reenter(ctx, scope, held)
	}
	for held := parent; held != nil; held = held.parent {
		if !held.active.Load() || !held.matches(s, string(encoded)) {
			continue
		}
		if !held.retainLease() {
			continue
		}
		return reenter(held)
	}
	if resources != nil {
		for _, held := range resources.guards {
			if !held.matches(s, string(encoded)) {
				continue
			}
			if held.acquiring {
				return ctx, nil, &Error{Code: CodeBadHandle, Message: "concurrent guard admission in one transaction"}
			}
			if held.active.Load() && held.retainLease() {
				return reenter(held)
			}
		}
	}
	held := &sealingGuard{sealing: s, provider: s.Keys, scope: string(encoded), parent: parent, acquiring: true}
	if resources != nil {
		resources.guards = append(resources.guards, held)
	}
	release, err := func() (func(), error) {
		if resources != nil {
			// Provider I/O must not hold the transaction resource mutex.
			resources.mu.Unlock()
			defer resources.mu.Lock()
		}
		return s.Keys.Hold(ctx, scope)
	}()
	held.acquiring = false
	if err != nil {
		return ctx, nil, err
	}
	if err := checkTransactionResources(ctx); err != nil {
		if resources != nil {
			resources.mu.Unlock()
		}
		release()
		if resources != nil {
			resources.mu.Lock()
		}
		return ctx, nil, err
	}
	held.release = release
	held.refs.Store(1)
	held.active.Store(true)
	guarded := context.WithValue(ctx, sealingGuardKey{}, held)
	return guarded, held.releaseLease(), nil
}

func (held *sealingGuard) matches(sealing *Sealing, scope string) bool {
	if held.sealing != sealing && !sameKeyProvider(held.provider, sealing.Keys) {
		return false
	}
	_, global := sealing.Keys.(memoryKeyFence)
	return global || held.scope == scope
}

func (s *Sealing) reenter(ctx context.Context, scope KeyScope, held *sealingGuard) (context.Context, func(), error) {
	lease := held.releaseLease()
	status, err := s.Keys.Status(ctx, scope)
	if err == nil {
		err = kernel.CheckKeyWriteStatus(status)
	}
	if err == nil {
		err = checkTransactionResources(ctx)
	}
	if err != nil {
		lease()
		return ctx, nil, err
	}
	return context.WithValue(ctx, sealingGuardKey{}, held), lease, nil
}

func (held *sealingGuard) retainLease() bool {
	for {
		count := held.refs.Load()
		if count <= 0 {
			return false
		}
		if held.refs.CompareAndSwap(count, count+1) {
			return true
		}
	}
}

func (held *sealingGuard) releaseLease() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			if held.refs.Add(-1) == 0 && held.active.Swap(false) {
				held.release()
			}
		})
	}
}

func sameKeyProvider(left, right KeyProvider) bool {
	if fence, ok := left.(memoryKeyFence); ok {
		left = fence.memoryKeyFence()
	}
	if fence, ok := right.(memoryKeyFence); ok {
		right = fence.memoryKeyFence()
	}
	typ := reflect.TypeOf(left)
	return typ != nil && typ.Comparable() && typ == reflect.TypeOf(right) && left == right
}

func (s *Sealing) prepare(ctx context.Context, tenant, subject *string) (EncryptionInfo, []byte, error) {
	scope, err := s.Scope(tenant, subject)
	if err != nil {
		return EncryptionInfo{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return EncryptionInfo{}, nil, err
	}
	reserved, err := s.Keys.Reserve(ctx, scope, s.Cipher)
	if err != nil {
		clear(reserved.Key)
		return EncryptionInfo{}, nil, err
	}
	info, err := kernel.FreshEncryptionInfo(s.KeyNamespace, reserved.KeyID, s.Cipher)
	if err != nil {
		clear(reserved.Key)
		return EncryptionInfo{}, nil, err
	}
	return *info, reserved.Key, nil
}

func (s *Sealing) resolve(ctx context.Context, tenant, subject *string, info EncryptionInfo) ([]byte, error) {
	if _, err := s.Scope(tenant, subject); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scope, err := kernel.EncryptionScope(s.KeyNamespace, info, tenant, subject)
	if err != nil {
		return nil, err
	}
	key, err := s.Keys.Resolve(ctx, *scope, info.Cipher, info.KeyID)
	if err != nil {
		clear(key)
		return nil, err
	}
	status, err := s.Keys.Status(ctx, *scope)
	if err == nil {
		err = kernel.CheckKeyStatus(status)
	}
	if err != nil {
		clear(key)
		return nil, err
	}
	return key, nil
}

// SealEvent returns a newly sealed draft. The caller retains it unchanged across commit retries.
func (s *Sealing) SealEvent(ctx context.Context, draft EventDraft) (*EventDraft, error) {
	info, key, err := s.prepare(ctx, draft.Tenant, draft.Subject)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	return kernel.SealEvent(draft, info, key)
}

// OpenEvent authenticates before returning payload bytes and leaves the raw envelope untouched.
func (s *Sealing) OpenEvent(ctx context.Context, event EventMessage) ([]byte, error) {
	data, _, err := s.openEvent(ctx, event, false)
	return data, err
}

// OpenEventDelivery permits a skip only when provider resolution confirms destruction.
// A later handler error is never interpreted by this function.
func (s *Sealing) OpenEventDelivery(ctx context.Context, event EventMessage) ([]byte, bool, error) {
	return s.openEvent(ctx, event, true)
}

func (s *Sealing) openEvent(ctx context.Context, event EventMessage, delivery bool) ([]byte, bool, error) {
	if _, err := s.Scope(event.Tenant, event.Subject); err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if event.Encryption == nil {
		if err := s.CheckLegacy(ctx, event.Tenant, event.Subject); err != nil {
			if delivery && destroyedKey(err) {
				return nil, true, nil
			}
			return nil, false, err
		}
		return append([]byte(nil), event.Data...), false, nil
	}
	key, err := s.resolve(ctx, event.Tenant, event.Subject, *event.Encryption)
	defer clear(key)
	if err != nil {
		if delivery && destroyedKey(err) {
			return nil, true, nil
		}
		return nil, false, err
	}
	data, err := kernel.OpenEvent(event, key)
	return data, false, err
}

// SealCommand returns a new envelope after the producer finalized every authenticated field.
func (s *Sealing) SealCommand(ctx context.Context, command CommandMessage) (*CommandMessage, error) {
	info, key, err := s.prepare(ctx, command.Tenant, command.Subject)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	return kernel.SealCommand(command, info, key)
}

// OpenCommand authenticates a command before decoding or cached-result replay.
func (s *Sealing) OpenCommand(ctx context.Context, command CommandMessage) ([]byte, error) {
	data, _, err := s.openCommand(ctx, command, false)
	return data, err
}

// OpenCommandDelivery permits a skip only from confirmed provider destruction.
func (s *Sealing) OpenCommandDelivery(ctx context.Context, command CommandMessage) ([]byte, bool, error) {
	return s.openCommand(ctx, command, true)
}

func (s *Sealing) openCommand(ctx context.Context, command CommandMessage, delivery bool) ([]byte, bool, error) {
	if _, err := s.Scope(command.Tenant, command.Subject); err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if command.Encryption == nil {
		if err := s.CheckLegacy(ctx, command.Tenant, command.Subject); err != nil {
			if delivery && destroyedKey(err) {
				return nil, true, nil
			}
			return nil, false, err
		}
		return append([]byte(nil), command.Data...), false, nil
	}
	key, err := s.resolve(ctx, command.Tenant, command.Subject, *command.Encryption)
	defer clear(key)
	if err != nil {
		if delivery && destroyedKey(err) {
			return nil, true, nil
		}
		return nil, false, err
	}
	data, err := kernel.OpenCommand(command, key)
	return data, false, err
}

func destroyedKey(err error) bool {
	code := kernel.CodeOf(err)
	return code != nil && *code == string(kernel.CodeShredded)
}

// StateContext names the immutable public context of a snapshot or saga state record.
type StateContext struct {
	Kind      string
	Identity  string
	TypeName  string
	Version   uint64
	Tenant    *string
	Subject   *string
	Marks     string
	Completed bool
}

func (state StateContext) context(info EncryptionInfo) ([]byte, error) {
	return kernel.StateEncryptionContext(state.Kind, state.Identity, state.TypeName, state.Version, state.Marks, state.Completed, info, state.Tenant, state.Subject)
}

// SealState encrypts serialized snapshot/saga application state with kernel-owned context.
func (s *Sealing) SealState(ctx context.Context, state StateContext, plaintext []byte) (*SealedValue, error) {
	info, key, err := s.prepare(ctx, state.Tenant, state.Subject)
	if err != nil {
		return nil, err
	}
	defer clear(key)
	context, err := state.context(info)
	if err != nil {
		return nil, err
	}
	data, err := kernel.SealBytes(info, key, context, plaintext)
	if err != nil {
		return nil, err
	}
	return &SealedValue{Encryption: &info, Data: data}, nil
}

// OpenState authenticates serialized state before the SDK decodes it.
func (s *Sealing) OpenState(ctx context.Context, state StateContext, info *EncryptionInfo, data []byte) ([]byte, error) {
	if _, err := s.Scope(state.Tenant, state.Subject); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if info == nil {
		if err := s.CheckLegacy(ctx, state.Tenant, state.Subject); err != nil {
			return nil, err
		}
		return append([]byte(nil), data...), nil
	}
	key, err := s.resolve(ctx, state.Tenant, state.Subject, *info)
	defer clear(key)
	if err != nil {
		return nil, err
	}
	context, err := state.context(*info)
	if err != nil {
		return nil, err
	}
	return kernel.OpenBytes(*info, key, context, data)
}

var _ KeyProvider = (*MemoryKeys)(nil)
