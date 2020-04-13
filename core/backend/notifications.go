package backend

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/relabs-tech/backends/core"
)

// Notification is a database notification. Receive them
// with RequestNotification()
type Notification struct {
	Serial       int
	Resource     string
	Operation    core.Operation
	State        string
	ResourceID   uuid.UUID
	Payload      []byte
	CreatedAt    time.Time
	AttemptsLeft int
}

type txNotification struct {
	tx           *sql.Tx
	notification Notification
}

func (b *Backend) handleNotifications() {
	_, err := b.db.Exec(`CREATE table IF NOT EXISTS ` + b.db.Schema + `."_notification_" 
(serial SERIAL,
resource VARCHAR NOT NULL, 
operation VARCHAR NOT NULL, 
state VARCHAR NOT NULL, 
resource_id uuid NOT NULL, 
payload JSON NOT NULL,
created_at TIMESTAMP NOT NULL, 
attempts_left INTEGER NOT NULL,
PRIMARY KEY(serial)
);`)

	if err != nil {
		panic(err)
	}

	log.Println("processing pipelines")
	log.Println("  handle route: /process GET")

	b.notificationsUpdateQuery = `UPDATE ` + b.db.Schema + `."_notification_"
SET attempts_left = attempts_left - 1
WHERE serial = (
SELECT serial
 FROM ` + b.db.Schema + `."_notification_"
 WHERE attempts_left > 0
 ORDER BY attempts_left, serial
 FOR UPDATE SKIP LOCKED
 LIMIT 1
)
RETURNING *;
`
	b.notificationsDeleteQuery = `DELETE FROM ` + b.db.Schema + `."_notification_"
WHERE serial = $1 RETURNING serial;`

}

func callWithPanicEnvelope(callback func(Notification) error, notification Notification) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %s", r)
		}
	}()
	err = callback(notification)
	return
}

func (b *Backend) pipelineWorker(n int, wg *sync.WaitGroup, jobs chan txNotification, output chan string) {
	defer wg.Done()

	for job := range jobs {
		tx := job.tx
		notification := job.notification

		request := notificationRequestKey(notification.Resource, notification.State, notification.Operation)

		if handler, ok := b.handlers[request]; ok {

			err := callWithPanicEnvelope(handler.callback, notification)
			if err != nil {
				output <- "error processing #" + strconv.Itoa(notification.Serial) + " " + request + ": " + err.Error()
				tx.Commit()
			} else {
				// notification handled sucessfully, delete form queue
				var serial int
				err = tx.QueryRow(b.notificationsDeleteQuery, &notification.Serial).Scan(&serial)
				if err == nil {
					err = tx.Commit()
				}
				if err != nil {
					output <- "error committing #" + strconv.Itoa(serial) + " " + request + ": " + err.Error()
				} else {
					output <- "successfully handled #" + strconv.Itoa(serial) + " " + request
				}
			}
		} else {
			// this should not happen
			output <- "no handler for #" + strconv.Itoa(notification.Serial) + " " + request
			tx.Commit()
		}
	}
}

// TriggerNotifications triggers pipeline processing by eventually calling ProcessPipelines().
// By default, processing happens in another go-routine, but by injecting another TriggerNotifications
// function it can also happen in its own lambda, triggered by an external queue event.
func (b *Backend) TriggerNotifications() {
	b.triggerNotifications()
}

// ProcessNotifications processes all pending notifications
func (b *Backend) ProcessNotifications() {
	log.Println("process pipelines")

	output := make(chan string, 100)
	collect := make(chan []string)

	go func() {
		var collected []string
		for s := range output {
			collected = append(collected, s)
		}
		collect <- collected
	}()

	jobs := make(chan txNotification, 20)
	var wg sync.WaitGroup
	wg.Add(b.pipelineConcurrency)
	for i := 0; i < b.pipelineConcurrency; i++ {
		go b.pipelineWorker(i, &wg, jobs, output)
	}

	for {
		tx, err := b.db.BeginTx(context.Background(), nil)
		if err != nil {
			output <- "failed to begin transaction: " + err.Error()
			break
		}

		var notification Notification
		err = tx.QueryRow(b.notificationsUpdateQuery).Scan(
			&notification.Serial,
			&notification.Resource,
			&notification.Operation,
			&notification.State,
			&notification.ResourceID,
			&notification.Payload,
			&notification.CreatedAt,
			&notification.AttemptsLeft,
		)

		if err != nil {
			if err != sql.ErrNoRows {
				output <- "failed to retrieve notification: " + err.Error()
			}
			tx.Rollback()
			break
		}
		jobs <- txNotification{tx, notification}
	}
	close(jobs)
	wg.Wait()
	close(output)
	collected := <-collect
	log.Print("processing report:\n  ", strings.Join(collected, "\n  "))
}

type notificationHandler struct {
	request  string
	callback func(Notification) error
}

// NotificationRequest represents a notification request
// for a specific resource in a specific state and
// a list of database operations
type NotificationRequest struct {
	Resource   string
	State      string
	Operations []core.Operation
}

// RequestNotifications requests database notifications and installs a handler for it.
//
// There can only be one handler for each unique combination of resource, state and operation.
//
// If a handler returns an error and the notification still has attempts left, then it will be rescheduled.
// The number of possible attempts is a configuration setting of the backend itself.
//
// The order of notifications is based on the number of attempts left (highest first)
func (b *Backend) RequestNotifications(handler func(Notification) error, requests ...NotificationRequest) {
	for _, request := range requests {
		for _, operation := range request.Operations {
			key := notificationRequestKey(request.Resource, request.State, operation)
			if _, ok := b.handlers[key]; ok {
				log.Fatalf("notification handler for %s already installed", key)
			}
			log.Printf("install notification handler %s", key)
			b.handlers[key] = notificationHandler{request: key, callback: handler}
		}
	}
}

func notificationRequestKey(resource string, state string, operation core.Operation) string {
	key := string(operation) + " " + resource
	if len(state) > 0 {
		key += " (" + state + ")"
	}
	return key
}

func (b *Backend) commitWithNotification(tx *sql.Tx, resource string, state string, operation core.Operation, resourceID uuid.UUID, payload []byte) error {
	request := notificationRequestKey(resource, state, operation)

	// only create a notification if somebody requested it
	if _, ok := b.handlers[request]; !ok {
		return tx.Commit()
	}

	if len(payload) == 0 {
		payload = []byte("{}")
	}

	var serial int
	err := tx.QueryRow("INSERT INTO "+b.db.Schema+".\"_notification_\""+
		"(resource,operation,state,resource_id,payload,created_at,attempts_left)"+
		"VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING serial;",
		resource,
		operation,
		state,
		resourceID,
		payload,
		time.Now().UTC(),
		b.pipelineMaxAttempts,
	).Scan(&serial)

	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err == nil {
		b.TriggerNotifications()
	}
	return err
}
