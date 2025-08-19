package test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type EventsOrderTestSuite struct {
	IntegrationTestSuite
}

func TestEventsOrderTestSuite(t *testing.T) {
	ts := &EventsOrderTestSuite{}
	suite.Run(t, ts)
}

func (s *EventsOrderTestSuite) TestEventOrdering() {
	mu := &sync.Mutex{}
	processedSequence := make(map[string][]int, 0)
	eventType := uuid.New().String()

	// Register handler that appends sequence number to slice
	s.HandleEvent(eventType, func(ctx context.Context, ev backend.Event) error {
		// Extract sequence number from payload
		// Assuming payload is a simple JSON number or raw bytes representing the number
		num := 0
		_ = json.Unmarshal(ev.Payload, &num)
		mu.Lock()
		processedSequence[ev.Key] = append(processedSequence[ev.Key], num)
		mu.Unlock()
		return nil
	})

	topicName := "event." + eventType
	err := s.createTopic(topicName, 3)
	s.Require().NoError(err, "Failed to create topic for test")
	defer s.deleteTopic(topicName)

	keys := []int{1, 2, 3, 4, 5}
	expectedSequences := make(map[string][]int, len(keys))
	for i := 0; i < 100; i++ {
		payload, _ := json.Marshal(i)
		job := backend.Event{
			Type:    eventType,
			Payload: payload,
			Key:     fmt.Sprintf("%d", keys[rand.Intn(len(keys))]),
		}
		expectedSequences[job.Key] = append(expectedSequences[job.Key], i)
		err := s.RaiseEvent(context.Background(), job)
		s.Require().NoError(err, "Failed to raise event")
	}

	s.ProcessJobsSync(time.Second * 10)

	// Wait until all 5 jobs are processed
	require.Eventually(s.T(), func() bool {
		var minLen int
		mu.Lock()
		for _, seq := range processedSequence {
			if len(seq) < minLen || minLen == 0 {
				minLen = len(seq)
			}
		}
		mu.Unlock()
		return minLen >= 10
	}, 10*time.Second, 100)

	mu.Lock()
	defer mu.Unlock()

	require.EqualValues(s.T(), expectedSequences, processedSequence, "Processed sequences do not match expected sequences")
	s.T().Log("All events processed successfully in order")
}

func (s *EventsOrderTestSuite) TestEventOrderingConsumerGroups() {
	mu := &sync.Mutex{}
	processedSequenceA := make(map[string][]int, 0)
	processedSequenceB := make(map[string][]int, 0)
	expectedSequenceA := make(map[string][]int, 0)
	expectedSequenceB := make(map[string][]int, 0)

	eventType := uuid.New().String()

	h := func(seq map[string][]int) func(ctx context.Context, ev backend.Event) error {
		return func(ctx context.Context, ev backend.Event) error {
			// Extract sequence number from payload
			// Assuming payload is a simple JSON number or raw bytes representing the number
			num := 0
			_ = json.Unmarshal(ev.Payload, &num)
			mu.Lock()
			seq[ev.Key] = append(seq[ev.Key], num)
			mu.Unlock()
			return nil
		}
	}

	topicName := "event." + eventType
	err := s.createTopic(topicName, 3)
	s.Require().NoError(err, "Failed to create topic for test")
	defer s.deleteTopic(topicName)

	s.HandleEvent(eventType, h(processedSequenceA), backend.WithConsumerGroup("groupA"))
	s.HandleEvent(eventType, h(processedSequenceB), backend.WithConsumerGroup("groupB"))

	keys := []int{1, 2, 3, 4, 5}
	for i := 0; i < 100; i++ {
		payload, _ := json.Marshal(i)
		job := backend.Event{
			Type:    eventType,
			Payload: payload,
			Key:     fmt.Sprintf("%d", keys[rand.Intn(len(keys))]),
		}
		expectedSequenceA[job.Key] = append(expectedSequenceA[job.Key], i)
		expectedSequenceB[job.Key] = append(expectedSequenceB[job.Key], i)
		err := s.RaiseEvent(context.Background(), job)
		s.Require().NoError(err, "Failed to raise event")
	}

	s.ProcessJobsSync(time.Second * 10)

	// Wait until all 5 jobs are processed
	require.Eventually(s.T(), func() bool {
		var minLen int
		mu.Lock()
		for _, seq := range processedSequenceA {
			if len(seq) < minLen || minLen == 0 {
				minLen = len(seq)
			}
		}
		mu.Unlock()
		return minLen >= 10
	}, 10*time.Second, 100)

	mu.Lock()
	defer mu.Unlock()
	require.EqualValues(s.T(), expectedSequenceA, processedSequenceA, "Processed sequences for group A do not match expected sequences")
	require.EqualValues(s.T(), expectedSequenceB, processedSequenceB, "Processed sequences for group B do not match expected sequences")

	s.T().Log("All events processed successfully in order")
}

func (s *EventsOrderTestSuite) TestNotificationOrdering() {
	mu := &sync.Mutex{}
	processedSequence := make(map[string][]int, 0)

	type seqItem struct {
		AID uuid.UUID `json:"a_id"`
		Seq int       `json:"seq"`
	}

	// Register handler that appends sequence number to slice
	s.HandleResourceNotification("a", func(ctx context.Context, ev backend.Notification) error {
		// Extract sequence number from payload
		// Assuming payload is a simple JSON number or raw bytes representing the number
		var item seqItem
		_ = json.Unmarshal(ev.Payload, &item)
		mu.Lock()
		processedSequence[ev.ResourceID.String()] = append(processedSequence[ev.ResourceID.String()], item.Seq)
		mu.Unlock()
		return nil
	})

	topicName := "notification.a"
	err := s.createTopic(topicName, 3)
	s.Require().NoError(err, "Failed to create topic for test")
	defer s.deleteTopic(topicName)

	keys := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	for i := 0; i < 100; i++ {
		item := seqItem{
			AID: keys[rand.Intn(len(keys))],
			Seq: i,
		}
		_, err := client.NewWithRouter(s.router).Collection("a").Upsert(item, nil)
		s.Require().NoError(err, "Failed to upsert item")
	}

	s.ProcessJobsSync(time.Second * 10)

	// Wait until all 5 jobs are processed
	require.Eventually(s.T(), func() bool {
		var minLen int
		mu.Lock()
		for _, seq := range processedSequence {
			if len(seq) < minLen || minLen == 0 {
				minLen = len(seq)
			}
		}
		mu.Unlock()
		return minLen >= 10
	}, 10*time.Second, 100)

	mu.Lock()
	defer mu.Unlock()
	for k, seq := range processedSequence {
		s.T().Logf("Key: %s, Processed Sequence: %v", k, seq)
		// Check if the sequence is in order
		for i := 0; i < len(seq)-1; i++ {
			require.LessOrEqual(s.T(), seq[i], seq[i+1], "Events for key %s are not processed in order", k)
		}
	}
	s.T().Log("All events processed successfully in order")
}

func (s *EventsOrderTestSuite) TestNotificationOrderingConsumerGroups() {
	mu := &sync.Mutex{}
	processedSequenceA := make(map[string][]int, 0)
	processedSequenceB := make(map[string][]int, 0)

	type seqItem struct {
		AID uuid.UUID `json:"a_id"`
		Seq int       `json:"seq"`
	}

	h := func(seq map[string][]int) func(ctx context.Context, ev backend.Notification) error {
		return func(ctx context.Context, ev backend.Notification) error {
			var item seqItem
			_ = json.Unmarshal(ev.Payload, &item)
			mu.Lock()
			seq[ev.ResourceID.String()] = append(seq[ev.ResourceID.String()], item.Seq)
			mu.Unlock()
			return nil
		}
	}

	topicName := "notification.a"
	err := s.createTopic(topicName, 3)
	s.Require().NoError(err, "Failed to create topic for test")
	defer s.deleteTopic(topicName)

	s.HandleResourceNotification("a->groupA", h(processedSequenceA))
	s.HandleResourceNotification("a->groupB", h(processedSequenceB))

	keys := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	for i := 0; i < 100; i++ {
		item := seqItem{
			AID: keys[rand.Intn(len(keys))],
			Seq: i,
		}
		_, err := client.NewWithRouter(s.router).Collection("a").Upsert(item, nil)
		s.Require().NoError(err, "Failed to upsert item")
	}

	s.ProcessJobsSync(time.Second * 10)

	// Wait until all 5 jobs are processed
	require.Eventually(s.T(), func() bool {
		var minLen int
		mu.Lock()
		for _, seq := range processedSequenceA {
			if len(seq) < minLen || minLen == 0 {
				minLen = len(seq)
			}
		}
		mu.Unlock()
		return minLen >= 10
	}, 10*time.Second, 100)

	mu.Lock()
	defer mu.Unlock()
	for k, seq := range processedSequenceA {
		s.T().Logf("Key: %s, Processed Sequence: %v", k, seq)
		// Check if the sequence is in order
		for i := 0; i < len(seq)-1; i++ {
			require.LessOrEqual(s.T(), seq[i], seq[i+1], "Events for key %s are not processed in order", k)
		}
	}
	for k, seq := range processedSequenceB {
		s.T().Logf("Key: %s, Processed Sequence: %v", k, seq)
		// Check if the sequence is in order
		for i := 0; i < len(seq)-1; i++ {
			require.LessOrEqual(s.T(), seq[i], seq[i+1], "Events for key %s are not processed in order", k)
		}
	}
	s.T().Log("All events processed successfully in order")
}

func (s *EventsOrderTestSuite) TestNotificationOrderingConsumerGroupsDifferentOperations() {
	mu := &sync.Mutex{}
	processedSequenceA := make(map[string][]int, 0)
	processedSequenceB := make(map[string][]int, 0)

	type seqItem struct {
		BID uuid.UUID `json:"b_id"`
		Seq int       `json:"seq"`
	}

	h := func(seq map[string][]int) func(ctx context.Context, ev backend.Notification) error {
		return func(ctx context.Context, ev backend.Notification) error {
			var item seqItem
			_ = json.Unmarshal(ev.Payload, &item)
			s.T().Logf("Processing event for resource %s with seq %d", ev.ResourceID, item.Seq)
			mu.Lock()
			seq[ev.ResourceID.String()] = append(seq[ev.ResourceID.String()], item.Seq)
			mu.Unlock()
			return nil
		}
	}

	topicName := "notification.b"
	err := s.createTopic(topicName, 3)
	s.Require().NoError(err, "Failed to create topic for test")
	defer s.deleteTopic(topicName)

	s.HandleResourceNotification("b->groupA", h(processedSequenceA), core.OperationUpdate)
	s.HandleResourceNotification("b->groupB", h(processedSequenceB), core.OperationCreate)

	keys := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	for i := 0; i < 100; i++ {
		item := seqItem{
			BID: keys[rand.Intn(len(keys))],
			Seq: i,
		}
		_, err := client.NewWithRouter(s.router).Collection("b").Upsert(item, nil)
		s.Require().NoError(err, "Failed to upsert item")
	}

	s.ProcessJobsSync(time.Second * 10)

	// Wait until all 5 jobs are processed
	require.Eventually(s.T(), func() bool {
		var minLen int
		mu.Lock()
		for _, seq := range processedSequenceA {
			if len(seq) < minLen || minLen == 0 {
				minLen = len(seq)
			}
		}
		mu.Unlock()
		return minLen >= 10
	}, 10*time.Second, 100)

	mu.Lock()
	defer mu.Unlock()
	for k, seq := range processedSequenceA {
		s.T().Logf("Key: %s, Processed Sequence: %v", k, seq)
		// Check if the sequence is in order
		for i := 0; i < len(seq)-1; i++ {
			require.LessOrEqual(s.T(), seq[i], seq[i+1], "Events for key %s are not processed in order", k)
		}
	}
	for k, seq := range processedSequenceB {
		s.T().Logf("Key: %s, Processed Sequence: %v", k, seq)
		// Check if the sequence is in order
		require.Equal(s.T(), len(seq), 1, "Events for key %s are not processed in order", k)
	}
	s.T().Log("All events processed successfully in order")
}
