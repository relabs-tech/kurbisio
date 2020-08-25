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
	Serial       int
	Resource     string
	Operation    core.Operation
	ResourceID   uuid.UUID
	Payload      []byte
	CreatedAt    time.Time
	AttemptsLeft int
	Context      context.Context
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
	Context      context.Context
}

// job can be a database notification or a highl-level event
type job struct {
	Serial       int
	Type         string
	Name         string
	Resource     string
	ResourceID   uuid.UUID
	Payload      []byte
	CreatedAt    time.Time
	AttemptsLeft int
	ContextData  []byte
}

// notification returns the job as database notification. Only makes sense if the job type is "notification"
func (j *job) notification() Notification {
	ctx := logger.ContextWithLoggerFromData(context.Background(), j.ContextData)
	return Notification{j.Serial, j.Resource, core.Operation(j.Name), j.ResourceID, j.Payload, j.CreatedAt, j.AttemptsLeft, ctx}
}

// event returns the job as high-level event. Only makes sense if the job type is "event"
func (j *job) event() Event {
	ctx := logger.ContextWithLoggerFromData(context.Background(), j.ContextData)
	return Event{j.Serial, j.Name, j.Resource, j.ResourceID, j.Payload, j.CreatedAt, j.AttemptsLeft, ctx}
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
resource VARCHAR NOT NULL DEFAULT '', 
resource_id uuid NOT NULL DEFAULT uuid_nil(), 
payload JSON NOT NULL DEFAULT'{}'::jsonb,
created_at TIMESTAMP NOT NULL DEFAULT now(), 
attempts_left INTEGER NOT NULL,
context JSON NOT NULL DEFAULT'{}'::jsonb,
PRIMARY KEY(serial),
CONSTRAINT job_compression UNIQUE(type,name,resource,resource_id)
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
RETURNING serial, type, name, resource, resource_id, payload, created_at, attempts_left, context;
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
	event := params["event"]
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
	status, err := b.raiseEventWithResourceInternal(r.Context(), event, resource, resourceID, payload)
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
			switch job.Type {
			case "notification":
				notification := job.notification()
				rlog = logger.FromContext(notification.Context)
				key = notificationJobKey(notification.Resource, notification.Operation)
				if handler, ok := b.callbacks[key]; ok {
					err = handler.notification(notification)
				} else {
					err = fmt.Errorf("no handler for key %s", key)
				}
			case "event":
				event := job.event()
				rlog = logger.FromContext(event.Context)
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
		err = tx.QueryRow(b.jobsUpdateQuery).Scan(
			&j.Serial,
			&j.Type,
			&j.Name,
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
//
// Multiple events of same kind will be compressed.
func (b *Backend) RaiseEvent(ctx context.Context, event string, payload interface{}) error {
	_, err := b.raiseEventWithResourceInternal(ctx, event, "", uuid.UUID{}, payload)
	return err
}

// RaiseEventWithResource raises the requested event. Payload can be nil, an object or a []byte.
// Callbacks registered with HandleEvent() will be called.
//
// Multiple events of the same kind to the very same resource (resource + resourceID) will be compressed,
// i.e. the newest payload will overwrite the previous payload.
func (b *Backend) RaiseEventWithResource(ctx context.Context, event string, resource string, resourceID uuid.UUID, payload interface{}) error {
	_, err := b.raiseEventWithResourceInternal(ctx, event, resource, resourceID, payload)
	return err
}

// raiseEventWithResourceInternal returns the http status code as well
func (b *Backend) raiseEventWithResourceInternal(ctx context.Context, event string, resource string, resourceID uuid.UUID, payload interface{}) (int, error) {
	key := eventJobKey(event)
	if _, ok := b.callbacks[key]; !ok {
		return http.StatusConflict, fmt.Errorf("no callback handler installed for %s", key)
	}
	var (
		ok          bool
		err         error
		data        []byte
		contextData []byte
	)
	if payload != nil {
		data, ok = payload.([]byte)
		if !ok {
			data, err = json.Marshal(payload)
			if err != nil {
				return http.StatusBadRequest, err
			}
		}
	} else {
		data = []byte("{}")
	}

	contextData = logger.SerializeLoggerContext(ctx)

	var serial int
	err = b.db.QueryRow("INSERT INTO "+b.db.Schema+".\"_job_\""+
		"(type,name,resource,resource_id,payload,created_at,attempts_left,context)"+
		"VALUES('event',$1,$2,$3,$4,$5,$6,$7) ON CONFLICT ON CONSTRAINT job_compression "+
		"DO UPDATE SET payload=$4,created_at=$5,attempts_left=$6,context=$7 RETURNING serial;",
		event,
		resource,
		resourceID,
		data,
		time.Now().UTC(),
		b.pipelineMaxAttempts,
		contextData,
	).Scan(&serial)

	if err != nil {
		return http.StatusInternalServerError, err
	}
	b.TriggerJobs()
	return http.StatusNoContent, nil
}

// HandleResource installs a callback handler for the given resource and the specified operations.
// If no operations are specified, the handler will be installed for all modifying operations, i.e. create,
// update and delete
func (b *Backend) HandleResource(resource string, handler func(Notification) error, operations ...core.Operation) {
	if len(operations) == 0 {
		operations = []core.Operation{core.OperationCreate, core.OperationUpdate, core.OperationDelete}
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

func timeoutJobKey(event string) string {
	return "timeout: " + event
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
		"(type, resource,name,resource_id,payload,created_at,attempts_left,context)"+
		"VALUES('notification',$1,$2,$3,$4,$5,$6,$7) ON CONFLICT ON CONSTRAINT job_compression "+
		"DO UPDATE SET payload=$4,created_at=$5,attempts_left=$6,context=$7 RETURNING serial;",
		resource,
		operation,
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
