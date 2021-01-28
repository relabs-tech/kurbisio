package backend

import (
	"context"
	"fmt"
	"testing"
	"time"

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
