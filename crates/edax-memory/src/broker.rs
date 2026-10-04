//! The in-memory broker: a partitioned log with consumer groups, settled through the Kafka kernel's
//! [`OffsetTracker`]. Not a store participant.
//!
//! It models what a log-based broker does to a handler, so a handler that only survives a queue
//! fails here and not first in production:
//! - a nack rewinds the partition, so the nacked message *and every later one*, acked or not, come
//!   back, in order (the tracker retires the copies already handed out, see
//!   [`is_live`](MemoryBroker::is_live));
//! - [`crash`](MemoryBroker::crash) drops a group's in-flight work and redelivers everything above
//!   its committed watermark, the way a consumer restart or a rebalance does;
//! - each consumer group reads every message, so a projection and a saga on one broker do not
//!   compete for them;
//! - order holds per partition, and so per key, not across keys once there are several partitions.
//!
//! It carries events and commands on one log, each placed by its ordering key, so a command and the
//! events of the aggregate it targets keep their relative order.
//!
//! A group commits on every ack that moves its watermark: in memory a commit costs nothing. Nothing
//! is ever deleted from the log, which suits a test and not a long-running process.

use std::collections::BTreeMap;
use std::sync::Mutex;

use edax_core::{lock, Error};
use edax_event::{CommandMessage, EventMessage, Message as _};
use edax_kafka::{partition_for, AckDecision, NackDecision, OffsetTracker};

/// The topic name the groups' trackers book under: the broker is one topic.
const TOPIC: &str = "memory";

/// A message on the log, of either kind: what a [`PolledDelivery`] carries.
#[derive(Clone, Debug, PartialEq)]
pub enum Message {
    /// An event.
    Event(EventMessage),
    /// A command.
    Command(CommandMessage),
}

impl Message {
    /// The key that places the message on the log, whichever its kind.
    pub fn ordering_key(&self) -> &str {
        match self {
            Message::Event(event) => event.ordering_key(),
            Message::Command(command) => command.ordering_key(),
        }
    }

    /// The message's own id, whichever its kind.
    pub fn id(&self) -> &str {
        match self {
            Message::Event(event) => event.id(),
            Message::Command(command) => command.id(),
        }
    }
}

/// A delivery handed out by [`MemoryBroker::try_poll`]: the message, where it sits in the log, and
/// the token that settles it.
#[derive(Clone, Debug, PartialEq)]
pub struct PolledDelivery {
    pub token: i64,
    pub partition: i32,
    pub offset: i64,
    /// The message delivered, which names its kind.
    pub message: Message,
}

/// One consumer group's view of the log: what it has fetched, what it has committed, and the
/// tracker that settles its deliveries.
struct Group {
    tracker: OffsetTracker,
    /// The next offset to fetch, per partition.
    position: Vec<i64>,
    /// The committed watermark, per partition: where a crash resumes.
    committed: Vec<i64>,
    /// The partition the next poll looks at first, so one busy partition cannot starve the others.
    next: usize,
}

impl Group {
    fn new(partitions: usize) -> Self {
        Self {
            tracker: OffsetTracker::new(),
            position: vec![0; partitions],
            committed: vec![0; partitions],
            next: 0,
        }
    }

    /// Commit what the tracker says is due. A commit here cannot fail, so it is recorded at once.
    fn commit(&mut self) {
        for due in self.tracker.due() {
            if let Some(committed) = usize::try_from(due.partition).ok().and_then(|p| self.committed.get_mut(p)) {
                *committed = due.offset;
                self.tracker.committed(TOPIC, due.partition, due.offset);
            }
        }
    }
}

struct BrokerState {
    /// One append-only log per partition.
    log: Vec<Vec<Message>>,
    groups: BTreeMap<String, Group>,
}

/// In-memory broker state machine for development and tests, behind a single lock. Non-blocking by
/// design: [`try_poll`](Self::try_poll) returns at once, and each SDK adds only its own waiting (a
/// `Condvar`, an asyncio event) on top — the one part a broker cannot share, because a blocking call
/// across the binding would stall the host's event loop. Settlement is data, not a callback: a poll
/// hands out a token, and [`ack`](Self::ack) / [`nack`](Self::nack) settle by it.
///
/// [`is_empty`](Self::is_empty) is true only when a group has committed past every message, the
/// same "nothing left to do" a durable broker reports. Without that, a dispatcher that dropped a
/// delivery would pass here and lose the message against Kafka.
pub struct MemoryBroker {
    state: Mutex<BrokerState>,
}

impl Default for MemoryBroker {
    /// One partition: one total order, which is what a test that has not thought about partitions
    /// expects.
    fn default() -> Self {
        Self { state: Mutex::new(BrokerState { log: vec![Vec::new()], groups: BTreeMap::new() }) }
    }
}

impl MemoryBroker {
    /// A broker with one partition.
    pub fn new() -> Self {
        Self::default()
    }

    /// A broker with `partitions` partitions; a key lands on Kafka's partition for it
    /// ([`partition_for`]). `0` is a `codec` error.
    pub fn with_partitions(partitions: u32) -> Result<Self, Error> {
        // `partition_for` owns the refusal of zero partitions; asking it once keeps one message.
        partition_for(b"", partitions)?;
        let count = usize::try_from(partitions).map_err(Error::codec)?;
        Ok(Self { state: Mutex::new(BrokerState { log: vec![Vec::new(); count], groups: BTreeMap::new() }) })
    }

    /// Append `event` to the partition of its ordering key, its aggregate. Every group sees it.
    pub fn publish_event(&self, event: EventMessage) {
        self.publish(Message::Event(event));
    }

    /// Append `command` to the partition of its ordering key: its target aggregate, or its own id when
    /// it has none. Every group sees it.
    pub fn publish_command(&self, command: CommandMessage) {
        self.publish(Message::Command(command));
    }

    fn publish(&self, message: Message) {
        let mut state = lock(&self.state);
        let count = u32::try_from(state.log.len()).unwrap_or(u32::MAX);
        // The broker always has a partition, so `partition_for` cannot refuse; the fallback is the first.
        let partition = partition_for(message.ordering_key().as_bytes(), count).unwrap_or(0);
        if let Some(log) = usize::try_from(partition).ok().and_then(|p| state.log.get_mut(p)) {
            log.push(message);
        }
    }

    /// Fetch the next message for `group` as an unsettled delivery, or `None` at once when the
    /// group has fetched everything. A group polled for the first time starts at the beginning of
    /// the log, so a late subscriber still sees every message.
    pub fn try_poll(&self, group: &str) -> Result<Option<PolledDelivery>, Error> {
        let mut state = lock(&self.state);
        let BrokerState { log, groups } = &mut *state;
        let group = groups.entry(group.to_owned()).or_insert_with(|| Group::new(log.len()));
        for step in 0..log.len() {
            let p = (group.next + step) % log.len();
            let (Some(records), Some(position)) = (log.get(p), group.position.get_mut(p)) else {
                continue;
            };
            let Some(message) = usize::try_from(*position).ok().and_then(|o| records.get(o)) else {
                continue;
            };
            let partition = i32::try_from(p).map_err(Error::codec)?;
            let offset = *position;
            let token = group.tracker.book(TOPIC, partition, offset)?;
            *position = offset.saturating_add(1);
            group.next = (p + 1) % log.len();
            return Ok(Some(PolledDelivery { token, partition, offset, message: message.clone() }));
        }
        Ok(None)
    }

    /// Settle `token` of `group` as handled, and commit the group's watermark if it moved. `false`
    /// and nothing changes if the token is not live: already settled, never issued, or retired by a
    /// nack or a crash.
    pub fn ack(&self, group: &str, token: i64) -> bool {
        let mut state = lock(&self.state);
        let Some(group) = state.groups.get_mut(group) else {
            return false;
        };
        match group.tracker.ack(token) {
            AckDecision::Settled => {
                group.commit();
                true
            }
            AckDecision::Stale => false,
        }
    }

    /// Settle `token` of `group` as failed: the group's fetch position on that partition rewinds to
    /// the lowest unsettled offset, so it and every later message come back, and every delivery of
    /// the partition already handed out is retired. `false` and nothing changes if the token is not
    /// live.
    pub fn nack(&self, group: &str, token: i64) -> bool {
        let mut state = lock(&self.state);
        let Some(group) = state.groups.get_mut(group) else {
            return false;
        };
        match group.tracker.nack(token) {
            NackDecision::Seek { partition, offset } => {
                if let Some(position) = usize::try_from(partition).ok().and_then(|p| group.position.get_mut(p)) {
                    *position = offset;
                }
                true
            }
            NackDecision::Stale => false,
        }
    }

    /// Whether `token` of `group` still settles its delivery. A dispatcher skips a delivery that is
    /// not live: a nack or a crash retired it, and its message comes back by a later poll.
    pub fn is_live(&self, group: &str, token: i64) -> bool {
        lock(&self.state).groups.get(group).is_some_and(|group| group.tracker.is_live(token))
    }

    /// Simulate the death of `group`'s consumer, or a rebalance: every delivery it holds is retired,
    /// and every message above its committed watermark is fetched again. A group never polled is a
    /// no-op: it starts at the beginning anyway.
    pub fn crash(&self, group: &str) {
        let mut state = lock(&self.state);
        if let Some(group) = state.groups.get_mut(group) {
            for partition in 0..group.position.len() {
                group.tracker.reset(TOPIC, i32::try_from(partition).unwrap_or(i32::MAX));
            }
            group.position.clone_from(&group.committed);
        }
    }

    /// True only when `group` has committed past every message in the log: nothing queued, nothing
    /// in flight. A group never polled is empty only while the log is.
    pub fn is_empty(&self, group: &str) -> bool {
        let state = lock(&self.state);
        match state.groups.get(group) {
            Some(group) => state
                .log
                .iter()
                .zip(&group.committed)
                .all(|(records, &committed)| i64::try_from(records.len()).is_ok_and(|len| len == committed)),
            None => state.log.iter().all(Vec::is_empty),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edax_core::NO_TOKEN;

    use crate::testkit::{command, event};

    const G: &str = "projection";
    const H: &str = "saga";

    /// An event of aggregate `agg` with id `"{agg}-{seq}"`, so a key's order reads off the ids.
    fn keyed(agg: &str, seq: u64) -> EventMessage {
        EventMessage { aggregate_id: agg.to_owned(), ..event(&format!("{agg}-{seq}"), Some(seq)) }
    }

    /// The id of the message a delivery carries, whichever its kind.
    fn id_of(d: &PolledDelivery) -> &str {
        d.message.id()
    }

    /// One step of a broker scenario. `Poll` records what `try_poll` returned for a group; the
    /// settle and liveness steps name the i-th delivery polled, in that delivery's own group.
    enum Step {
        Publish(&'static str, u64),
        Poll(&'static str),
        Ack(usize),
        Nack(usize),
        AckToken(&'static str, i64),
        NackToken(&'static str, i64),
        Live(usize),
        Crash(&'static str),
    }

    struct Case {
        name: &'static str,
        steps: &'static [Step],
        /// Message ids each `Poll` produced, in order; `None` for an empty poll.
        polled: &'static [Option<&'static str>],
        /// What each settle and liveness step returned, in order.
        answers: &'static [bool],
        /// `is_empty` per group at the end.
        empty: &'static [(&'static str, bool)],
    }

    fn run(broker: &MemoryBroker, case: &Case) {
        let mut held: Vec<(&str, PolledDelivery)> = Vec::new();
        let mut polled: Vec<Option<String>> = Vec::new();
        let mut answers: Vec<bool> = Vec::new();
        for step in case.steps {
            match step {
                Step::Publish(agg, seq) => broker.publish_event(keyed(agg, *seq)),
                Step::Poll(group) => match broker.try_poll(group).expect("a poll never fails here") {
                    Some(d) => {
                        polled.push(Some(id_of(&d).to_owned()));
                        held.push((group, d));
                    }
                    None => polled.push(None),
                },
                Step::Ack(i) => answers.push(broker.ack(held[*i].0, held[*i].1.token)),
                Step::Nack(i) => answers.push(broker.nack(held[*i].0, held[*i].1.token)),
                Step::AckToken(group, token) => answers.push(broker.ack(group, *token)),
                Step::NackToken(group, token) => answers.push(broker.nack(group, *token)),
                Step::Live(i) => answers.push(broker.is_live(held[*i].0, held[*i].1.token)),
                Step::Crash(group) => broker.crash(group),
            }
        }
        let want: Vec<Option<String>> = case.polled.iter().map(|id| id.map(str::to_owned)).collect();
        assert_eq!(polled, want, "{}: polled ids", case.name);
        assert_eq!(answers, case.answers, "{}: settle and liveness answers", case.name);
        for (group, empty) in case.empty {
            assert_eq!(broker.is_empty(group), *empty, "{}: is_empty({group})", case.name);
        }
    }

    #[test]
    fn broker_branches() {
        use Step::*;
        let cases = [
            Case {
                name: "a new broker is empty and try_poll returns nothing at once",
                steps: &[Poll(G)],
                polled: &[None],
                answers: &[],
                empty: &[(G, true)],
            },
            Case {
                name: "a published event makes every group non-empty, polled or not",
                steps: &[Publish("a", 1)],
                polled: &[],
                answers: &[],
                empty: &[(G, false), (H, false)],
            },
            Case {
                name: "a polled delivery is unsettled, so still non-empty",
                steps: &[Publish("a", 1), Poll(G)],
                polled: &[Some("a-1")],
                answers: &[],
                empty: &[(G, false)],
            },
            Case {
                name: "ack settles and commits it",
                steps: &[Publish("a", 1), Poll(G), Ack(0)],
                polled: &[Some("a-1")],
                answers: &[true],
                empty: &[(G, true)],
            },
            Case {
                // Kafka's seek-back: the nacked message and the later one, already acked, both come
                // back, in order. The duplicate of an acked message is the at-least-once hazard a
                // queue that requeues at the tail never produces.
                name: "a nack rewinds and redelivers every later message, acked or not, in order",
                steps: &[Publish("a", 1), Publish("a", 2), Poll(G), Poll(G), Ack(1), Nack(0), Poll(G), Poll(G), Poll(G)],
                polled: &[Some("a-1"), Some("a-2"), Some("a-1"), Some("a-2"), None],
                answers: &[true, true],
                empty: &[(G, false)],
            },
            Case {
                // The copy of a-2 handed out before the rewind is dead: running it would apply a-2
                // ahead of the redelivered a-1.
                name: "a nack retires every delivery of its partition already handed out",
                steps: &[Publish("a", 1), Publish("a", 2), Poll(G), Poll(G), Nack(0), Live(1), Ack(1), Nack(1)],
                polled: &[Some("a-1"), Some("a-2")],
                answers: &[true, false, false, false],
                empty: &[(G, false)],
            },
            Case {
                name: "a redelivered copy is live and settles",
                steps: &[Publish("a", 1), Poll(G), Nack(0), Poll(G), Live(1), Ack(1)],
                polled: &[Some("a-1"), Some("a-1")],
                answers: &[true, true, true],
                empty: &[(G, true)],
            },
            Case {
                // Fails against a set of message ids: one ack would clear the single entry and the
                // broker would report drained with a delivery still outstanding.
                name: "the same id published twice needs two settlements",
                steps: &[Publish("a", 1), Publish("a", 1), Poll(G), Poll(G), Ack(0)],
                polled: &[Some("a-1"), Some("a-1")],
                answers: &[true],
                empty: &[(G, false)],
            },
            Case {
                name: "the same id published twice is drained by two settlements",
                steps: &[Publish("a", 1), Publish("a", 1), Poll(G), Poll(G), Ack(1), Ack(0)],
                polled: &[Some("a-1"), Some("a-1")],
                answers: &[true, true],
                empty: &[(G, true)],
            },
            Case {
                name: "settling the same delivery twice is refused and loses nothing",
                steps: &[Publish("a", 1), Publish("a", 2), Poll(G), Poll(G), Ack(0), Ack(0)],
                polled: &[Some("a-1"), Some("a-2")],
                answers: &[true, false],
                empty: &[(G, false)],
            },
            Case {
                // At-least-once: a stale ack after a nack must not drop the redelivery.
                name: "ack after nack of the same token does not drop the redelivery",
                steps: &[Publish("a", 1), Poll(G), Nack(0), Ack(0), Poll(G)],
                polled: &[Some("a-1"), Some("a-1")],
                answers: &[true, false],
                empty: &[(G, false)],
            },
            Case {
                name: "nack twice rewinds once",
                steps: &[Publish("a", 1), Poll(G), Nack(0), Nack(0), Poll(G), Poll(G)],
                polled: &[Some("a-1"), Some("a-1"), None],
                answers: &[true, false],
                empty: &[(G, false)],
            },
            Case {
                name: "a token no poll issued settles nothing, in a group or outside any",
                steps: &[
                    Publish("a", 1),
                    Poll(G),
                    AckToken(G, 999),
                    AckToken(G, NO_TOKEN),
                    AckToken("nobody", 1),
                    NackToken(G, 999),
                    NackToken("nobody", 1),
                ],
                polled: &[Some("a-1")],
                answers: &[false, false, false, false, false],
                empty: &[(G, false)],
            },
            Case {
                name: "one partition keeps one total order across keys",
                steps: &[Publish("a", 1), Publish("b", 1), Publish("a", 2), Poll(G), Poll(G), Poll(G)],
                polled: &[Some("a-1"), Some("b-1"), Some("a-2")],
                answers: &[],
                empty: &[(G, false)],
            },
            Case {
                // The consumer died holding a-2 and a-3: their tokens are dead, and the group
                // resumes from its commit, so both come back and a-1 does not.
                name: "a crash redelivers everything above the committed watermark",
                steps: &[
                    Publish("a", 1),
                    Publish("a", 2),
                    Publish("a", 3),
                    Poll(G),
                    Poll(G),
                    Poll(G),
                    Ack(0),
                    Crash(G),
                    Live(1),
                    Ack(2),
                    Poll(G),
                    Poll(G),
                    Poll(G),
                ],
                polled: &[Some("a-1"), Some("a-2"), Some("a-3"), Some("a-2"), Some("a-3"), None],
                answers: &[true, false, false],
                empty: &[(G, false)],
            },
            Case {
                // Acked out of order, so the watermark stayed at the unsettled a-1: the crash
                // redelivers a-2 too, which was handled — the duplicate a restart produces.
                name: "a crash redelivers an acked message above an unsettled one",
                steps: &[Publish("a", 1), Publish("b", 1), Poll(G), Poll(G), Ack(1), Crash(G), Poll(G), Poll(G)],
                polled: &[Some("a-1"), Some("b-1"), Some("a-1"), Some("b-1")],
                answers: &[true],
                empty: &[(G, false)],
            },
            Case {
                name: "a crash of a group never polled is a no-op",
                steps: &[Publish("a", 1), Crash(G), Poll(G)],
                polled: &[Some("a-1")],
                answers: &[],
                empty: &[(G, false)],
            },
            Case {
                // Fan-out: a projection and a saga each get every message, and settle on their own.
                name: "every group reads every message",
                steps: &[Publish("a", 1), Poll(G), Poll(H), Ack(0), Poll(G), Poll(H)],
                polled: &[Some("a-1"), Some("a-1"), None, None],
                answers: &[true],
                empty: &[(G, true), (H, false)],
            },
            Case {
                name: "a token is its own group's: another group's ack settles nothing",
                steps: &[Publish("a", 1), Poll(G), Poll(H), AckToken(H, 999), Ack(1)],
                polled: &[Some("a-1"), Some("a-1")],
                answers: &[false, true],
                empty: &[(G, false), (H, true)],
            },
            Case {
                name: "a group polled late starts at the beginning",
                steps: &[Publish("a", 1), Publish("a", 2), Poll(G), Ack(0), Poll(G), Ack(1), Poll(H), Poll(H)],
                polled: &[Some("a-1"), Some("a-2"), Some("a-1"), Some("a-2")],
                answers: &[true, true],
                empty: &[(G, true), (H, false)],
            },
        ];
        for case in &cases {
            run(&MemoryBroker::new(), case);
        }
    }

    #[test]
    fn partitions_keep_order_per_key_and_rewind_alone() {
        assert_eq!(
            MemoryBroker::with_partitions(0).err().map(|e| e.to_string()),
            edax_kafka::partition_for(b"", 0).err().map(|e| e.to_string()),
            "zero partitions is the placement kernel's refusal"
        );
        let broker = MemoryBroker::with_partitions(4).expect("four partitions");
        // Kafka's placement, so a key lands where a Java producer would put it.
        let keys = ["a", "b", "c", "d", "e", "f"];
        for seq in 1..=3 {
            for key in keys {
                broker.publish_event(keyed(key, seq));
            }
        }
        let mut seen: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut held: Vec<PolledDelivery> = Vec::new();
        while let Some(d) = broker.try_poll(G).expect("poll") {
            let Message::Event(event) = &d.message else { panic!("only events were published") };
            seen.entry(event.aggregate_id.clone()).or_default().push(event.id.clone());
            let want = u32::try_from(d.partition).expect("partition");
            assert_eq!(partition_for(event.aggregate_id.as_bytes(), 4).ok(), Some(want), "Kafka's placement");
            held.push(d);
        }
        for key in keys {
            assert_eq!(seen[key], [format!("{key}-1"), format!("{key}-2"), format!("{key}-3")], "per-key order");
        }
        // Round robin: the first polls visit every partition that holds a message before any repeats.
        let used: std::collections::BTreeSet<i32> = held.iter().map(|d| d.partition).collect();
        let first_round: std::collections::BTreeSet<i32> = held.iter().take(used.len()).map(|d| d.partition).collect();
        assert!(used.len() > 1, "the keys spread over several partitions");
        assert_eq!(first_round, used, "one busy partition does not starve the others");

        // A nack rewinds its own partition only: the other partitions' deliveries stay live.
        let first = held.first().expect("polled").clone();
        assert!(broker.nack(G, first.token));
        for d in &held[1..] {
            assert_eq!(broker.is_live(G, d.token), d.partition != first.partition, "{}", id_of(d));
        }
    }

    #[test]
    fn broker_tokens_are_unique_per_delivery_and_never_the_placeholder() {
        let broker = MemoryBroker::new();
        broker.publish_event(event("evt-1", Some(1)));
        let first = broker.try_poll(G).expect("poll").expect("queued");
        assert_ne!(first.token, NO_TOKEN, "a live delivery never carries the placeholder token");
        assert_eq!((first.partition, first.offset), (0, 0));
        assert_eq!(first.message, Message::Event(event("evt-1", Some(1))), "the event crosses field for field");

        // A redelivery is a new delivery: it settles by its own token, not the stale one.
        assert!(broker.nack(G, first.token));
        let again = broker.try_poll(G).expect("poll").expect("redelivered");
        assert_ne!(again.token, first.token);
        assert!(!broker.ack(G, first.token), "the stale token is dead");
        assert!(broker.ack(G, again.token));
        assert!(broker.is_empty(G));
    }

    /// The log and the accounting sit behind one lock, so contending pollers of one group each get
    /// a distinct delivery and nothing is handed out twice or lost.
    #[test]
    fn concurrent_polls_hand_each_message_out_once() {
        let broker = MemoryBroker::new();
        for seq in 1..=4 {
            broker.publish_event(event(&format!("evt-{seq}"), Some(seq)));
        }
        let mut deliveries: Vec<PolledDelivery> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8).map(|_| s.spawn(|| broker.try_poll(G).expect("poll"))).collect();
            handles.into_iter().filter_map(|h| h.join().expect("poller thread")).collect()
        });
        deliveries.sort_by_key(|d| d.offset);
        assert_eq!(deliveries.iter().map(id_of).collect::<Vec<_>>(), ["evt-1", "evt-2", "evt-3", "evt-4"]);
        assert!(!broker.is_empty(G), "all four are unsettled");
        for d in &deliveries {
            assert!(broker.ack(G, d.token));
        }
        assert!(broker.is_empty(G));
    }

    #[test]
    fn commands_share_the_log_and_the_placement_with_events() {
        // Four partitions, so placement is visible. A command lands where its ordering key puts it:
        // beside its target aggregate's events, in publish order, or on its own id when it targets none.
        let broker = MemoryBroker::with_partitions(4).expect("four partitions");
        broker.publish_event(keyed("invoice-7", 1));
        broker.publish_command(command("cmd-1", Some("invoice-7")));
        broker.publish_event(keyed("invoice-7", 2));
        // `cmd-3` lands on another partition than `invoice-7`, so the two placements are told apart.
        broker.publish_command(command("cmd-3", None));
        let mut held: Vec<PolledDelivery> = Vec::new();
        while let Some(d) = broker.try_poll(G).expect("poll") {
            held.push(d);
        }
        let placed = |key: &str| partition_for(key.as_bytes(), 4).ok().and_then(|p| i32::try_from(p).ok());
        let on_invoice: Vec<&str> = held.iter().filter(|d| Some(d.partition) == placed("invoice-7")).map(id_of).collect();
        assert_eq!(on_invoice, ["invoice-7-1", "cmd-1", "invoice-7-2"], "a command keeps its order among its aggregate's events");
        let untargeted = held.iter().find(|d| id_of(d) == "cmd-3").expect("delivered");
        assert_eq!(Some(untargeted.partition), placed("cmd-3"), "a command with no target is placed by its id");
        assert_ne!(placed("cmd-3"), placed("invoice-7"));

        // The delivery names its kind, and the command crosses field for field.
        let cmd = held.iter().find(|d| id_of(d) == "cmd-1").expect("delivered");
        assert_eq!(cmd.message, Message::Command(command("cmd-1", Some("invoice-7"))));
        for d in &held {
            assert!(broker.ack(G, d.token));
        }
        assert!(broker.is_empty(G), "a command settles like an event");
    }
}
