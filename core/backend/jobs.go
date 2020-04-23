package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
)

// Notification is a database notification. Receive them
// with HandleResource()
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

// Event is a higher level event. Receive them with HandleEvent(), raise them with RaiseEvent()
type Event struct {
	Serial       int
	Name         string
	Resource     string
	ResourceID   uuid.UUID
	Payload      []byte
	CreatedAt    time.Time
	AttemptsLeft int
}

// job can be a database notification or a highl-level event
type job struct {
	Serial       int
	Type         string
	Name         string
	Scheduled    time.Time
	Resource     string
	State        string
	ResourceID   uuid.UUID
	Payload      []byte
	CreatedAt    time.Time
	AttemptsLeft int
}

// notification returns the job as database notification. Only makes sense if the job type is "notification"
func (j *job) notification() Notification {
	return Notification{j.Serial, j.Resource, core.Operation(j.Name), j.State, j.ResourceID, j.Payload, j.CreatedAt, j.AttemptsLeft}
}

// event returns the job as high-level event. Only makes sense if the job type is "event"
func (j *job) event() Event {
	return Event{j.Serial, j.Name, j.Resource, j.ResourceID, j.Payload, j.CreatedAt, j.AttemptsLeft}
}

type txJob struct {
	job
	tx *sql.Tx
}

func (b *Backend) handleJobs(router *mux.Router) {
	if b.updateSchema {
		_, err := b.db.Exec(`CREATE table IF NOT EXISTS ` + b.db.Schema + `."_job_" 
(serial SERIAL,
type VARCHAR NOT NULL,
name VARCHAR NOT NULL DEFAULT '',
scheduled TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',
resource VARCHAR NOT NULL DEFAULT '', 
state VARCHAR NOT NULL DEFAULT '', 
resource_id uuid NOT NULL DEFAULT uuid_nil(), 
payload JSON NOT NULL DEFAULT'{}'::jsonb,
created_at TIMESTAMP NOT NULL DEFAULT now(), 
attempts_left INTEGER NOT NULL,
PRIMARY KEY(serial),
CONSTRAINT job_compression UNIQUE(type,name,resource,state,resource_id)
);`)

		if err != nil {
			panic(err)
		}
	}

	b.jobsUpdateQuery = `UPDATE ` + b.db.Schema + `."_job_"
SET attempts_left = attempts_left - 1
WHERE serial = (
SELECT serial
 FROM ` + b.db.Schema + `."_job_"
 WHERE attempts_left > 0
 ORDER BY attempts_left, serial
 FOR UPDATE SKIP LOCKED
 LIMIT 1
)
RETURNING *;
`
	b.jobsDeleteQuery = `DELETE FROM ` + b.db.Schema + `."_job_"
WHERE serial = $1 RETURNING serial;`

	log.Println("job processing pipelines")
	log.Println("  handle route: /kurbisio/events PUT")

	router.HandleFunc("/kurbisio/events/{event}", func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		b.eventsWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)
}

func (b *Backend) eventsWithAuth(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	event := params["event"]
	if b.authorizationEnabled {
		auth := access.AuthorizationFromContext(r.Context())
		if !auth.HasRole("admin") {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}
	}
	var (
		resource   string
		resourceID uuid.UUID
	)
	urlQuery := r.URL.Query()
	for key, array := range urlQuery {
		var err error
		if len(array) > 1 {
			http.Error(w, "illegal parameter array '"+key+"'", http.StatusBadRequest)
			return
		}
		value := array[0]
		switch key {
		case "resource":
			resource = value
		case "resource_id":
			resourceID, err = uuid.Parse(value)
		default:
			err = fmt.Errorf("unknown query parameter")
		}
		if err != nil {
			http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var payload interface{}
	if len(body) > 0 { // we do not want to pass an empty []byte
		payload = body
	}
	status, err := b.raiseEventWithResourceInternal(event, resource, resourceID, payload)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	log.Printf("raised event %s on resource \"%s\"", event, resource)
}

func (b *Backend) pipelineWorker(n int, wg *sync.WaitGroup, jobs chan txJob) {
	defer wg.Done()

	for job := range jobs {
		tx := job.tx
		var key string

		// call the registered handler in a panic/recover envelope
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("recovered from panic: %s", r)
				}
			}()
			switch job.Type {
			case "notification":
				notification := job.notification()
				key = notificationJobKey(notification.Resource, notification.State, notification.Operation)
				if handler, ok := b.callbacks[key]; ok {
					err = handler.notification(notification)
				} else {
					err = fmt.Errorf("no handler for key %s", key)
				}
			case "event":
				event := job.event()
				key = eventJobKey(event.Name)
				if handler, ok := b.callbacks[key]; ok {
					err = handler.event(event)
				} else {
					err = fmt.Errorf("no handler for key %s", key)
				}
			default:
				err = fmt.Errorf("unknown job type %s", job.Type)
			}
			return
		}()

		if err != nil {
			log.Println("error processing "+key+"#"+strconv.Itoa(job.Serial)+":", err.Error())
			tx.Commit()
		} else {
			// job handled sucessfully, delete form queue
			var serial int
			err = tx.QueryRow(b.jobsDeleteQuery, &job.Serial).Scan(&serial)
			if err == nil {
				err = tx.Commit()
			}
			if err != nil {
				log.Println("error committing "+key+"#"+strconv.Itoa(serial)+":", err.Error())
			} else {
				log.Println(" successfully handled " + key + "#" + strconv.Itoa(serial))
			}
		}
	}
}

// TriggerJobs triggers pipeline processing by eventually calling ProcessJobs().
// By default, processing happens in another go-routine, but by injecting another TriggerJobs
// function it can also happen in its own lambda, triggered by an external queue event.
func (b *Backend) TriggerJobs() {
	b.triggerJobs()
}

// ProcessJobs processes all pending jobs and then returns true if there are still stops to do
func (b *Backend) ProcessJobs() bool {
	log.Println("process jobs")

	jobs := make(chan txJob, b.pipelineConcurrency)
	var wg sync.WaitGroup
	wg.Add(b.pipelineConcurrency)
	for i := 0; i < b.pipelineConcurrency; i++ {
		go b.pipelineWorker(i, &wg, jobs)
	}

	maxJobs := 1000
	for {
		tx, err := b.db.BeginTx(context.Background(), nil)
		if err != nil {
			log.Println("failed to begin transaction:", err.Error())
			break
		}

		var j job
		err = tx.QueryRow(b.jobsUpdateQuery).Scan(
			&j.Serial,
			&j.Type,
			&j.Name,
			&j.Scheduled,
			&j.Resource,
			&j.State,
			&j.ResourceID,
			&j.Payload,
			&j.CreatedAt,
			&j.AttemptsLeft,
		)

		if err != nil {
			if err != sql.ErrNoRows {
				log.Println("failed to retrieve job:", err.Error())
			}
			tx.Rollback()
			break
		}
		jobs <- txJob{j, tx}
		maxJobs--
		if maxJobs == 0 {
			b.TriggerJobs() // request continuation
			break
		}
	}
	close(jobs)
	wg.Wait()
	log.Println("process jobs done")
	return maxJobs == 0
}

type jobHandler struct {
	notification func(Notification) error
	event        func(Event) error
}

// HandleEvent installs a callback handler the specified event
func (b *Backend) HandleEvent(event string, handler func(Event) error) {
	key := eventJobKey(event)
	if _, ok := b.callbacks[key]; ok {
		log.Fatalf("callback handler for %s already installed", key)
	}
	b.callbacks[key] = jobHandler{event: handler}
}

// RaiseEvent raises the requested event. Payload can be nil, an object or a []byte.
// Callbacks registered with HandleEvent() will be called.
func (b *Backend) RaiseEvent(event string, payload interface{}) error {
	_, err := b.raiseEventWithResourceInternal(event, "", uuid.UUID{}, payload)
	return err
}

// RaiseEventWithResource raises the requested event. Payload can be nil, an object or a []byte.
// Callbacks registered with HandleEvent() will be called.
func (b *Backend) RaiseEventWithResource(event string, resource string, resourceID uuid.UUID, payload interface{}) error {
	_, err := b.raiseEventWithResourceInternal(event, resource, resourceID, payload)
	return err
}

// raiseEventWithResourceInternal returns the http status code as well
func (b *Backend) raiseEventWithResourceInternal(event string, resource string, resourceID uuid.UUID, payload interface{}) (int, error) {
	key := eventJobKey(event)
	if _, ok := b.callbacks[key]; !ok {
		return http.StatusConflict, fmt.Errorf("no callback handler installed for %s", key)
	}
	var (
		ok   bool
		err  error
		data []byte
	)
	if payload != nil {
		data, ok = payload.([]byte)
		if !ok {
			data, err = json.MarshalIndent(payload, "", "  ")
			if err != nil {
				return http.StatusBadRequest, err
			}
		}
	} else {
		data = []byte("{}")
	}
	var serial int
	err = b.db.QueryRow("INSERT INTO "+b.db.Schema+".\"_job_\""+
		"(type,name,resource,resource_id,payload,created_at,attempts_left)"+
		"VALUES('event',$1,$2,$3,$4,$5,$6) ON CONFLICT ON CONSTRAINT job_compression "+
		"DO UPDATE SET payload=$4,created_at=$5,attempts_left=$6 RETURNING serial;",
		event,
		resource,
		resourceID,
		data,
		time.Now().UTC(),
		b.pipelineMaxAttempts,
	).Scan(&serial)

	if err == nil {
		b.triggerJobs()
	}

	return http.StatusInternalServerError, err
}

// HandleResource installs a callback handler for the given resource and state and the specified operations.
// If no operations are specified, the handler will be installed for all modifying operations, i.e. create,
// update and delete
func (b *Backend) HandleResource(resource string, state string, handler func(Notification) error, operations ...core.Operation) {
	if len(operations) == 0 {
		operations = []core.Operation{core.OperationCreate, core.OperationUpdate, core.OperationDelete}
	}
	for _, operation := range operations {
		key := notificationJobKey(resource, state, operation)
		if _, ok := b.callbacks[key]; ok {
			log.Fatalf("callback handler for %s already installed", key)
		}
		log.Printf("install callback handler for %s", key)
		b.callbacks[key] = jobHandler{notification: handler}
	}
}

func notificationJobKey(resource string, state string, operation core.Operation) string {
	key := "notification: " + resource + "(" + string(operation) + ")"
	if len(state) > 0 {
		key += "[" + state + "]"
	}
	return key
}

func eventJobKey(event string) string {
	return "event: " + event
}

func timeoutJobKey(event string) string {
	return "timeout: " + event
}

func (b *Backend) commitWithNotification(tx *sql.Tx, resource string, state string, operation core.Operation, resourceID uuid.UUID, payload []byte) error {
	request := notificationJobKey(resource, state, operation)

	// only create a notification if somebody requested it
	if _, ok := b.callbacks[request]; !ok {
		return tx.Commit()
	}

	if len(payload) == 0 {
		payload = []byte("{}")
	}

	var serial int
	err := tx.QueryRow("INSERT INTO "+b.db.Schema+".\"_job_\""+
		"(type, resource,name,state,resource_id,payload,created_at,attempts_left)"+
		"VALUES('notification',$1,$2,$3,$4,$5,$6,$7) ON CONFLICT ON CONSTRAINT job_compression "+
		"DO UPDATE SET payload=$5,created_at=$6,attempts_left=$7 RETURNING serial;",
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
		b.TriggerJobs()
	}
	return err
}
