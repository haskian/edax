package edax

// Version separates the stored stream version from pending changes.
type Version struct {
	Initial uint64
	Updates uint64
}

// Current includes both the stored stream version and pending events.
func (v Version) Current() uint64 { return v.Initial + v.Updates }

// Aggregate tracks pending events. Embed it in a domain aggregate and implement Transition.
// An aggregate belongs to one caller; it is not safe for concurrent mutation.
type Aggregate struct {
	id      string
	tenant  *string
	initial uint64
	pending []any
}

// NewAggregate starts an empty stream with the supplied domain identifier.
func NewAggregate(id string) Aggregate { return Aggregate{id: id} }

// AggregateState exposes tracking state for repository operations.
func (a *Aggregate) AggregateState() *Aggregate { return a }

// ID returns the domain identifier, without any tenant prefix.
func (a *Aggregate) ID() string { return a.id }

// Tenant returns a copy of the tenant identifier, or nil for an unscoped aggregate.
func (a *Aggregate) Tenant() *string {
	if a.tenant == nil {
		return nil
	}
	return Ptr(*a.tenant)
}

// Version reports the stored version and number of pending events.
func (a *Aggregate) Version() Version {
	return Version{a.initial, uint64(len(a.pending))}
}

// Pending returns a shallow copy of the pending event slice; event values remain shared.
func (a *Aggregate) Pending() []any { return append([]any(nil), a.pending...) }

// AggregateRoot applies domain events without recording them again during replay.
type AggregateRoot interface {
	AggregateState() *Aggregate
	Transition(any) error
}

// Apply changes state and records each successful event for the next save.
func Apply(aggregate AggregateRoot, events ...any) error {
	for _, event := range events {
		if err := aggregate.Transition(event); err != nil {
			return err
		}
		state := aggregate.AggregateState()
		state.pending = append(state.pending, event)
	}
	return nil
}

func standAt(aggregate AggregateRoot, version uint64, tenant *string) {
	state := aggregate.AggregateState()
	state.initial = version
	state.pending = nil
	state.tenant = nil
	if tenant != nil {
		state.tenant = Ptr(*tenant)
	}
}
