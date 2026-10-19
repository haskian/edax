//! A delayed broker reply must not acknowledge a replacement partition's progress.

use edax_kafka::{AckDecision, Due, NackDecision, OffsetTracker};

const TOPIC: &str = "order-events";

#[allow(clippy::unwrap_used, reason = "test fixture must produce a due commit")]
fn settled(tracker: &OffsetTracker, offset: i64) -> Due {
    let token = tracker.book(TOPIC, 0, offset).unwrap();
    assert_eq!(tracker.ack(token), AckDecision::Settled);
    tracker.due().into_iter().next().unwrap()
}

fn complete(tracker: &OffsetTracker, due: &Due) {
    tracker.committed_generation(&due.topic, due.partition, due.offset, due.generation);
}

#[test]
fn completion_is_fenced_after_reset_or_revoke_and_rebooking() {
    for revoke in [false, true] {
        let tracker = OffsetTracker::new();
        let old = settled(&tracker, 100);
        if revoke {
            assert_eq!(tracker.revoke(TOPIC, 0), Some(old.clone()));
        } else {
            tracker.reset(TOPIC, 0);
        }
        complete(&tracker, &old);
        assert!(tracker.stats().is_empty());

        let current = settled(&tracker, 10);
        assert_ne!(old.generation, current.generation);
        complete(&tracker, &old);
        assert_eq!(tracker.stats()[0].committed, None);
        assert_eq!(tracker.due(), vec![current.clone()]);

        complete(&tracker, &current);
        assert_eq!(tracker.stats()[0].committed, Some(11));
        assert!(tracker.due().is_empty());
    }
}

#[test]
fn rewind_retires_commit_generation_as_well_as_delivery_tokens() {
    let tracker = OffsetTracker::new();
    let old = settled(&tracker, 10);
    let failed = tracker.book(TOPIC, 0, 11).unwrap();
    let sibling = tracker.book(TOPIC, 0, 12).unwrap();
    assert_eq!(
        tracker.nack(failed),
        NackDecision::Seek {
            partition: 0,
            offset: 11
        }
    );
    assert!(!tracker.is_live(sibling));

    complete(&tracker, &old);
    assert_eq!(tracker.stats()[0].committed, None);
    let current = tracker.due().into_iter().next().unwrap();
    assert_ne!(current.generation, old.generation);
    assert_eq!(current.offset, old.offset);

    // A stale nack must not invalidate the current generation again.
    assert_eq!(tracker.nack(sibling), NackDecision::Stale);
    assert_eq!(tracker.due(), vec![current.clone()]);
    complete(&tracker, &current);
    assert_eq!(tracker.stats()[0].committed, Some(11));
    assert!(tracker.due().is_empty());
}

#[test]
fn matching_completions_max_merge_without_affecting_other_partitions() {
    let tracker = OffsetTracker::new();
    let first = settled(&tracker, 10);
    let next = settled(&tracker, 11);
    assert_eq!(first.generation, next.generation);
    let unrelated = tracker.book(TOPIC, 1, 50).unwrap();
    assert_eq!(tracker.ack(unrelated), AckDecision::Settled);

    complete(&tracker, &next);
    complete(&tracker, &first);
    let stats = tracker.stats();
    assert_eq!(stats[0].committed, Some(12));
    assert_eq!(stats[1].committed, None);
    assert_eq!(tracker.due().len(), 1);
    assert_eq!(tracker.due()[0].partition, 1);
}
