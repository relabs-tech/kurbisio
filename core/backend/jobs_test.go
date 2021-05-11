package backend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/relabs-tech/backends/core/access"
)

func TestPutEvent(t *testing.T) {
	eventType := "some-event"
	received := make(chan interface{}, 10)
	testService.backend.HandleEvent(eventType, func(ctx context.Context, event Event) error {
		fmt.Printf("%v", event)
		received <- nil
		return nil
	})

	cl := testService.client
	status, err := cl.RawPut("/kurbisio/events/"+eventType, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if status != 204 {
		t.Fatalf("Expecting status %d, got %d", 204, status)
	}

	testService.backend.ProcessJobsSync(-1)
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for event to be received")
	case <-received:
	}

	// We now try with a non admin client
	cl = testService.clientNoAuth
	status, _ = cl.RawPut("/kurbisio/events/"+eventType, nil, nil)
	if status != 401 {
		t.Fatalf("Expecting status %d, got %d", 401, status)
	}

	testService.backend.ProcessJobsSync(-1)
	select {
	case <-time.After(100 * time.Millisecond):
	case <-received:
		t.Fatal("Have received an event")
	}

	// We now try with an admin and 'admin viewer' client
	cl = testService.client.WithAuthorization(&access.Authorization{Roles: []string{"admin viewer", "admin"}})
	status, _ = cl.RawPut("/kurbisio/events/"+eventType, nil, nil)
	if status != 204 {
		t.Fatalf("Expecting status %d, got %d", 204, status)
	}

	testService.backend.ProcessJobsSync(-1)
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Have not received an event")
	case <-received:
	}
}

func TestEventRetry(t *testing.T) {
	eventType := "retry-event"
	received := make(chan *time.Time, 10)
	testService.backend.HandleEvent(eventType, func(ctx context.Context, event Event) error {
		received <- event.ScheduledAt
		return fmt.Errorf("this fails")
	})
	rec0 := time.Now().UTC()
	err := testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}

	timeouts := [3]time.Duration{time.Second, time.Second * 2, time.Second * 3}
	testService.backend.ProcessJobsSyncWithTimeouts(-1, timeouts)
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		if r != nil {
			t.Fatal("raised event has scheduled_at, but should not")
		}
	}

	time.Sleep(timeouts[0])
	testService.backend.ProcessJobsSyncWithTimeouts(-1, timeouts)
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(timeouts[0])); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(timeouts[0])); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
		rec0 = rec
	}
	time.Sleep(timeouts[1])
	testService.backend.ProcessJobsSyncWithTimeouts(-1, timeouts)
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(timeouts[1])); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(timeouts[1])); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
		rec0 = rec
	}
	time.Sleep(timeouts[2])
	testService.backend.ProcessJobsSyncWithTimeouts(-1, timeouts)
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(timeouts[2])); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(timeouts[2])); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
		rec0 = rec
	}

	// that's it, no more retries, we have failed
	health, _ := testService.backend.Health(true /*include details*/)
	found := false
	for _, j := range health.Jobs.Details {
		found = j.Job == "event" && j.Type == "retry-event" && j.AttemptsLeft == 0
	}

	if !found {
		t.Fatal("no failed event in jobs table")
	}

}

func TestRateLimitEvent(t *testing.T) {
	eventType := "rate-limited-event"
	received := make(chan *time.Time, 10)
	testService.backend.HandleEvent(eventType, func(ctx context.Context, event Event) error {
		received <- event.ScheduledAt
		return nil
	})

	delta := 1000 * time.Millisecond
	testService.backend.DefineRateLimitForEvent(eventType, delta, time.Minute)
	rec0 := time.Now().UTC()
	err := testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}
	err = testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}
	err = testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}
	err = testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}

	testService.backend.ProcessJobsSync(-1)
	select {
	case <-time.After(delta):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(0 * delta)); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(0 * delta)); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
		rec0 = rec
	}
	time.Sleep(delta)
	testService.backend.ProcessJobsSync(-1)
	select {
	case <-time.After(delta):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(1 * delta)); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(1 * delta)); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
		rec0 = rec
	}
	time.Sleep(delta)
	testService.backend.ProcessJobsSync(-1)
	select {
	case <-time.After(delta):
		t.Fatal("Timeout waiting for event to be received")
	case <-received:
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(1 * delta)); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(1 * delta)); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
		rec0 = rec
	}
	time.Sleep(delta)
	testService.backend.ProcessJobsSync(-1)
	select {
	case <-time.After(delta):
		t.Fatal("Timeout waiting for event to be received")
	case <-received:
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(1 * delta)); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(1 * delta)); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
	}
}

func TestRateLimitEventRetry(t *testing.T) {
	eventType := "rate-limited-event-retry"
	received := make(chan *time.Time, 10)
	testService.backend.HandleEvent(eventType, func(ctx context.Context, event Event) error {
		received <- event.ScheduledAt
		return fmt.Errorf("this fails")
	})

	delta := 500 * time.Millisecond
	testService.backend.DefineRateLimitForEvent(eventType, delta, time.Minute)
	rec0 := time.Now().UTC()
	err := testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}

	timeouts := [3]time.Duration{200 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond}
	testService.backend.ProcessJobsSyncWithTimeouts(-1, timeouts)
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(0 * delta)); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(0 * delta)); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
		rec0 = rec
	}
	time.Sleep(200 * time.Millisecond)
	// now the retry should fire (after 200ms), but since the event is rate limited, it should be put on the +200ms schedule from the original time.
	testService.backend.ProcessJobsSyncWithTimeouts(-1, timeouts)

	select {
	case <-time.After(delta - 200*time.Millisecond):
	case <-received:
		t.Fatal("Have received an event")
	}

	// now the rescheduled event with rate limited schedule should fire
	testService.backend.ProcessJobsSyncWithTimeouts(-1, timeouts)
	select {
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(1 * delta)); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(1 * delta)); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
	}
}

func TestRateLimitEventMaxAge(t *testing.T) {
	eventType := "rate-limited-event-maxage"
	received := make(chan *time.Time, 10)
	testService.backend.HandleEvent(eventType, func(ctx context.Context, event Event) error {
		received <- event.ScheduledAt
		return nil
	})

	delta := 200 * time.Millisecond
	maxAge := 200 * time.Millisecond
	testService.backend.DefineRateLimitForEvent(eventType, delta, maxAge)
	err := testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}
	err = testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}
	err = testService.backend.RaiseEvent(context.TODO(), Event{Type: eventType, Resource: "something", ResourceID: uuid.New()})
	if err != nil {
		t.Fatalf("raise event error: %v", err)
	}

	testService.backend.ProcessJobsSync(-1)
	select {
	case <-time.After(delta):
		t.Fatal("Timeout waiting for event to be received")
	case <-received:
	}

	// now we simulate the server not being responsive
	time.Sleep(maxAge + 3*delta)
	// and continue processing. The rate limited events is now older than max age and should be rescheduled by the system
	rec0 := time.Now().UTC()
	testService.backend.ProcessJobsSync(-1)

	select {
	case <-time.After(delta):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(0 * delta)); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(0 * delta)); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
	}

	time.Sleep(delta)
	testService.backend.ProcessJobsSync(-1)

	select {
	case <-time.After(delta):
		t.Fatal("Timeout waiting for event to be received")
	case r := <-received:
		rec := *r
		if d := rec.Sub(rec0.Add(1 * delta)); d < 0 {
			t.Fatalf("event too early: %v", d)
		}
		if d := rec.Sub(rec0.Add(1 * delta)); d > 50*time.Millisecond {
			t.Fatalf("event too late: %v", d)
		}
	}
}
