package kernel

import "testing"

func TestCommitGenerationSurvivesTheBinding(t *testing.T) {
	tracker := GetOffsetTracker()
	t.Cleanup(func() { _ = tracker.Close() })
	book := func(offset int64) int64 {
		t.Helper()
		token, err := tracker.Book("accounts", 0, offset)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	ack := func(token int64) {
		t.Helper()
		settled, err := tracker.Ack(token)
		if err != nil || !settled {
			t.Fatalf("ack: settled=%v err=%v", settled, err)
		}
	}
	ack(book(100))
	old, err := tracker.Due()
	if err != nil || len(old) != 1 {
		t.Fatalf("old due: %v, %v", old, err)
	}
	if err := tracker.Reset("accounts", 0); err != nil {
		t.Fatal(err)
	}
	ack(book(0))
	if err := tracker.CommittedGeneration("accounts", 0, old[0].Offset, old[0].Generation); err != nil {
		t.Fatal(err)
	}
	fresh, err := tracker.Due()
	if err != nil || len(fresh) != 1 || fresh[0].Offset != 1 || fresh[0].Generation == old[0].Generation {
		t.Fatalf("stale completion affected new generation: %v, %v", fresh, err)
	}
	if err := tracker.CommittedGeneration("accounts", 0, fresh[0].Offset, fresh[0].Generation); err != nil {
		t.Fatal(err)
	}
	remaining, err := tracker.Due()
	if err != nil || len(remaining) != 0 {
		t.Fatalf("current completion ignored: %v, %v", remaining, err)
	}
}
