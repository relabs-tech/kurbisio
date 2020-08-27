package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/logger"
)

// Notification is a database notification. Receive them
// with HandleResource()
type Notification struct {
	Resource   string
	ResourceID uuid.UUID
	Operation  core.Operation
	Payload    []byte
}

// Event is a higher level event. Receive them with HandleEvent(), raise them with RaiseEvent(), scehdule them with ScheduleEvent()
type Event struct {
	Type       string
	Key        string
	Resource   string
	ResourceID uuid.UUID
	Payload    []byte
}

// WithPayload adds a payload to an event. Payload can be an object or a []byte
func (e Event) WithPayload(payload interface{}) Event {
	data, ok := payload.([]byte)
	if !ok {
		data, _ = json.Marshal(payload)
	}
	e.Payload = data
	return e
}

// job can be a database notification or a highl-level event
type job struct {
	Serial       int
	Job          string
	Type         string
	Key          string
	Resource     string
	ResourceID   uuid.UUID
	Payload      []byte
	CreatedAt    time.Time
	AttemptsLeft int
	ContextData  []byte
}

// notification returns the job as database notification. Only makes sense if the job type is "notification"
func (j *job) notification() (Notification, context.Context) {
	ctx := logger.ContextWithLoggerFromData(context.Background(), j.ContextData)
	return Notification{Resource: j.Resource, Operation: core.Operation(j.Type), ResourceID: j.ResourceID, Payload: j.Payload}, ctx
}

// event returns the job as high-level event. Only makes sense if the job type is "event"
func (j *job) event() (Event, context.Context) {
	ctx := logger.ContextWithLoggerFromData(context.Background(), j.ContextData)
	return Event{Type: j.Type, Key: j.Key, Resource: j.Resource, ResourceID: j.ResourceID, Payload: j.Payload}, ctx
}

type txJob struct {
	job
	tx *sql.Tx
}

func (b *Backend) handleJobs(router *mux.Router) {
	if b.updateSchema {
		_, err := b.db.Exec(`CREATE table IF NOT EXISTS ` + b.db.Schema + `."_job_" 
(serial SERIAL,
job VARCHAR NOT NULL,
type VARCHAR NOT NULL DEFAULT '',
key VARCHAR NOT NULL DEFAULT '',
resource VARCHAR NOT NULL DEFAULT '', 
resource_id uuid NOT NULL DEFAULT uuid_nil(), 
payload JSON NOT NULL DEFAULT'{}'::jsonb,
created_at TIMESTAMP NOT NULL DEFAULT now(), 
attempts_left INTEGER NOT NULL,
context JSON NOT NULL DEFAULT'{}'::jsonb,
scheduled_at TIMESTAMP,
PRIMARY KEY(serial)
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_event_compression ON ` + b.db.Schema + `._job_(type,key,resource,resource_id) WHERE job = 'event';
CREATE index IF NOT EXISTS jobs_scheduled_at_index ON ` + b.db.Schema + `._job_(scheduled_at);
`)

		if err != nil {
			panic(err)
		}
	}

	b.jobsUpdateQuery = `UPDATE ` + b.db.Schema + `."_job_"
SET attempts_left = attempts_left - 1
WHERE serial = (
SELECT serial
 FROM ` + b.db.Schema + `."_job_"
 WHERE attempts_left > 0 AND (scheduled_at IS NULL OR $1 > scheduled_at)
 ORDER BY attempts_left, serial
 FOR UPDATE SKIP LOCKED
 LIMIT 1
)
RETURNING serial, job, type, key, resource, resource_id, payload, created_at, attempts_left, context;
`
	b.jobsDeleteQuery = `DELETE FROM ` + b.db.Schema + `."_job_"
WHERE serial = $1 RETURNING serial;`

	logger.Default().Infoln("job processing pipelines")
	logger.Default().Infoln("  handle route: /kurbisio/events PUT")

	router.HandleFunc("/kurbisio/events/{event}", func(w http.ResponseWriter, r *http.Request) {
		logger.Default().Infoln("called route for", r.URL, r.Method)
		b.eventsWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)
}

func (b *Backend) eventsWithAuth(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	eventType := params["event"]
	rlog := logger.FromContext(r.Context())
	rlog.Infoln("in events with auth")
	if b.authorizationEnabled {
		auth := access.AuthorizationFromContext(r.Context())
		if !auth.HasRole("admin") {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}
	}
	var (
		key        string
		resource   string
		resourceID uuid.UUID
	)
	urlQuery := r.URL.Query()
	for param, array := range urlQuery {
		var err error
		if len(array) > 1 {
			http.Error(w, "illegal parameter array '"+param+"'", http.StatusBadRequest)
			return
		}
		value := array[0]
		switch param {
		case "key":
			param = value
		case "resource":
			resource = value
		case "resource_id":
			resourceID, err = uuid.Parse(value)
		default:
			err = fmt.Errorf("unknown query parameter")
		}
		if err != nil {
			http.Error(w, "parameter '"+param+"': "+err.Error(), http.StatusBadRequest)
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
	event := Event{Type: eventType, Key: key, Resource: resource, ResourceID: resourceID}.WithPayload(payload)
	status, err := b.raiseEventWithResourceInternal(r.Context(), event, nil)

	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	w.WriteHeader(status)
	rlog.Infof("raised event %s on resource \"%s\"", event, resource)
}

func (b *Backend) pipelineWorker(n int, wg *sync.WaitGroup, jobs chan txJob) {
	defer wg.Done()

	for job := range jobs {
		tx := job.tx
		var key string
		rlog := logger.Default()

		// call the registered handler in a panic/recover envelope
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("recovered from panic: %s", r)
					debug.PrintStack()
				}
			}()
			switch job.Job {
			case "notification":
				notification, ctx := job.notification()
				rlog = logger.FromContext(ctx)
				key = notificationJobKey(notification.Resource, notification.Operation)
				if handler, ok := b.callbacks[key]; ok {
					err = handler.notification(ctx, notification)
				} else {
					err = fmt.Errorf("no handler for key %s", key)
				}
			case "event":
				event, ctx := job.event()
				rlog = logger.FromContext(ctx)
				key = eventJobKey(event.Type)
				if handler, ok := b.callbacks[key]; ok {
					err = handler.event(ctx, event)
				} else {
					err = fmt.Errorf("no handler for key %s", key)
				}
			default:
				err = fmt.Errorf("unknown job type %s", job.Job)
			}
			return
		}()

		if err != nil {
			rlog.Errorln("error processing "+key+"#"+strconv.Itoa(job.Serial)+":", err.Error())
			tx.Commit()
		} else {
			// job handled sucessfully, delete form queue
			var serial int
			err = tx.QueryRow(b.jobsDeleteQuery, &job.Serial).Scan(&serial)
			if err == nil {
				err = tx.Commit()
			}
			if err != nil {
				rlog.Errorln("error committing "+key+"#"+strconv.Itoa(serial)+":", err.Error())
			} else {
				rlog.Infoln(" successfully handled " + key + "#" + strconv.Itoa(serial))
			}
		}
	}
}

// TriggerJobs triggers pipeline processing.
func (b *Backend) TriggerJobs() {
	b.hasJobsToProcessLock.Lock()
	b.hasJobsToProcess = true
	b.hasJobsToProcessLock.Unlock()
	if b.processJobsAsyncRuns {
		if len(b.processJobsAsyncTrigger) == 0 {
			b.processJobsAsyncTrigger <- struct{}{}
		}

	}
}

// HasJobsToProcess returns true, if there are jobs to process.
// It then resets the process flag.
func (b *Backend) HasJobsToProcess() bool {
	b.hasJobsToProcessLock.Lock()
	defer b.hasJobsToProcessLock.Unlock()
	result := b.hasJobsToProcess
	b.hasJobsToProcess = false
	return result
}

// ProcessJobsAsync starts a job processing loop. It returns immediately. This
// function must only be called once.
// The function triggers processing of left-over jobs in the database right away.
func (b *Backend) ProcessJobsAsync() {
	if b.processJobsAsyncRuns {
		panic("already processing jobs")
	}
	b.processJobsAsyncRuns = true
	b.processJobsAsyncTrigger = make(chan struct{}, 10)

	go func() {
		b.ProcessJobsSync(-1)
		for {
			<-b.processJobsAsyncTrigger
			if b.HasJobsToProcess() {
				b.ProcessJobsSync(-1)
			}
		}
	}()

}

// ProcessJobsSync processes all pending jobs up to the specified mximum and then returns. It returns true if it
// has maxed out and there are more jobs to process, otherwise it returns false.
// It you pass -1, it will process all pending jobs. The behaviour for 0 is undefined.
func (b *Backend) ProcessJobsSync(max int) bool {
	rlog := logger.FromContext(nil)
	rlog.Infoln("process jobs")
	jobCount := 0

process:
	b.HasJobsToProcess() // reset flag

	jobs := make(chan txJob, b.pipelineConcurrency)
	var wg sync.WaitGroup
	wg.Add(b.pipelineConcurrency)
	for i := 0; i < b.pipelineConcurrency; i++ {
		go b.pipelineWorker(i, &wg, jobs)
	}

	var maxedOut bool

	for {
		tx, err := b.db.BeginTx(context.Background(), nil)
		if err != nil {
			rlog.Errorln("failed to begin transaction:", err.Error())
			break
		}

		var j job
		now := time.Now().UTC()
		err = tx.QueryRow(b.jobsUpdateQuery, now).Scan(
			&j.Serial,
			&j.Job,
			&j.Type,
			&j.Key,
			&j.Resource,
			&j.ResourceID,
			&j.Payload,
			&j.CreatedAt,
			&j.AttemptsLeft,
			&j.ContextData,
		)

		if err != nil {
			if err != sql.ErrNoRows {
				rlog.Errorln("failed to retrieve job:", err.Error())
			}
			tx.Rollback()
			break
		}
		jobs <- txJob{j, tx}
		jobCount++
		if maxedOut = max >= 0 && jobCount >= max; maxedOut {
			break
		}
	}
	close(jobs)
	wg.Wait()

	if !maxedOut && b.HasJobsToProcess() {
		goto process // goto considered useful
	}

	rlog.Infof("process jobs done, did %d jobs (maxedOut == %t)", jobCount, maxedOut)
	return maxedOut
}

type jobHandler struct {
	notification func(context.Context, Notification) error
	event        func(context.Context, Event) error
}

// HandleEvent installs a callback handler the specified event
func (b *Backend) HandleEvent(event string, handler func(context.Context, Event) error) {
	key := eventJobKey(event)
	if _, ok := b.callbacks[key]; ok {
		log.Fatalf("callback handler for %s already installed", key)
	}
	b.callbacks[key] = jobHandler{event: handler}
}

// RaiseEvent raises the requested event. Payload can be nil, an object or a []byte.
// Callbacks registered with HandleEvent() will be called.
//
// Multiple events of the same kind (event plus key) to the very same resource (resource + resourceID) will be compressed,
// i.e. the newest payload will overwrite the previous payload. If you do not want any compression, use a unique key
//
// Use ScheduleEvent if you want to schedule an event at a specific time.
func (b *Backend) RaiseEvent(ctx context.Context, event Event) error {
	_, err := b.raiseEventWithResourceInternal(ctx, event, nil)
	return err
}

// ScheduleEvent schedules the requested event at a specific point in time. Payload can be nil, an object or a []byte.
// Callbacks registered with HandleEvent() will be called.
//
// Multiple events of the same kind (event plus key) to the very same resource (resource + resourceID) will be compressed,
// i.e. the newest payload will overwrite the previous payload. If you do not want any compression, use a unique key
//
// Use RaiseEvent if you want to raise the event immediately.
func (b *Backend) ScheduleEvent(ctx context.Context, event Event, scheduleAt time.Time) error {
	_, err := b.raiseEventWithResourceInternal(ctx, event, &scheduleAt)
	return err
}

// raiseEventWithResourceInternal returns the http status code as well
func (b *Backend) raiseEventWithResourceInternal(ctx context.Context, event Event, scheduleAt *time.Time) (int, error) {
	key := eventJobKey(event.Type)
	if _, ok := b.callbacks[key]; !ok {
		return http.StatusConflict, fmt.Errorf("no callback handler installed for %s", key)
	}
	var (
		err         error
		data        []byte
		contextData []byte
	)

	if event.Payload != nil {
		data = event.Payload
	} else {
		data = []byte("{}")
	}

	contextData = logger.SerializeLoggerContext(ctx)
	var scheduleAtUTC *time.Time
	if scheduleAt != nil {
		tmp := scheduleAt.UTC()
		scheduleAtUTC = &tmp
	}

	var serial int
	err = b.db.QueryRow("INSERT INTO "+b.db.Schema+".\"_job_\""+
		"(job,type,key,resource,resource_id,payload,created_at,attempts_left,context, scheduled_at)"+
		"VALUES('event',$1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (type,key,resource,resource_id) WHERE job = 'event' "+
		"DO UPDATE SET payload=$5,created_at=$6,attempts_left=$7,context=$8,scheduled_at=$9 RETURNING serial;",
		event.Type,
		event.Key,
		event.Resource,
		event.ResourceID,
		data,
		time.Now().UTC(),
		b.pipelineMaxAttempts,
		contextData,
		scheduleAtUTC,
	).Scan(&serial)

	if err != nil {
		return http.StatusInternalServerError, err
	}
	b.TriggerJobs()
	return http.StatusNoContent, nil
}

// HandleResource installs a callback handler for the given resource and the specified operations.
// If no operations are specified, the handler will be installed for all modifying operations, i.e. create,
// update, delete and clear
func (b *Backend) HandleResource(resource string, handler func(context.Context, Notification) error, operations ...core.Operation) {
	if len(operations) == 0 {
		operations = []core.Operation{core.OperationCreate, core.OperationUpdate, core.OperationDelete, core.OperationClear}
	}
	for _, operation := range operations {
		key := notificationJobKey(resource, operation)
		if _, ok := b.callbacks[key]; ok {
			logger.FromContext(nil).Fatalf("callback handler for %s already installed", key)
		}
		logger.FromContext(nil).Infof("install callback handler for %s", key)
		b.callbacks[key] = jobHandler{notification: handler}
	}
}

func notificationJobKey(resource string, operation core.Operation) string {
	key := "notification: " + resource + "(" + string(operation) + ")"
	return key
}

func eventJobKey(event string) string {
	return "event: " + event
}

func (b *Backend) commitWithNotification(ctx context.Context, tx *sql.Tx, resource string, operation core.Operation, resourceID uuid.UUID, payload []byte) error {
	rlog := logger.FromContext(ctx)
	rlog.Debugf("commitWithNotification START")
	request := notificationJobKey(resource, operation)

	// only create a notification if somebody requested it
	if _, ok := b.callbacks[request]; !ok {
		return tx.Commit()
	}

	if len(payload) == 0 {
		payload = []byte("{}")
	}

	contextData := logger.SerializeLoggerContext(ctx)

	rlog.Debugf("commitWithNotification before: tx.QueryRow")
	var serial int
	err := tx.QueryRow("INSERT INTO "+b.db.Schema+".\"_job_\""+
		"(job,type,resource,resource_id,payload,created_at,attempts_left,context)"+
		"VALUES('notification',$1,$2,$3,$4,$5,$6,$7) RETURNING serial;",
		operation,
		resource,
		resourceID,
		payload,
		time.Now().UTC(),
		b.pipelineMaxAttempts,
		contextData,
	).Scan(&serial)

	if err != nil {
		rlog.Debugf("commitWithNotification before: tx.Rollback()")
		tx.Rollback()
		return err
	}
	rlog.Debugf("commitWithNotification before: err = tx.Commit()")
	err = tx.Commit()
	rlog.Debugf("commitWithNotification after: err = tx.Commit()")
	if err == nil {
		b.TriggerJobs()
		rlog.Debugf("commitWithNotification after: b.TriggerJobs()")
	}
	rlog.Debugf("commitWithNotification END")
	return err
}
