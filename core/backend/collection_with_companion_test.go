package backend_test

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/backend"
	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/csql"
)

func TestCompanion(t *testing.T) {
	// Create a relation and verifies that the relation can be listed

	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_companion_unit_test_")
	defer db.Close()
	db.ClearSchema()

	var configurationJSON string = `{
		"collections": [			
			{
			  "resource":"release",
			  "permits": [
					{
						"role": "creator",
						"operations": [
							"create",
							"update"
						]
					},
					{
						"role": "reader",
						"operations": [
							"read",
							"list"
						]
					}
				]
			},
			{
			  "resource":"release/artefact",
			  "with_companion": true,
			  "permits": [
					{
						"role": "creator",
						"operations": [
							"create",
							"update"
						]
					},
					{
						"role": "reader",
						"operations": [
							"read",
							"list"
						]
					}
				]
			}	 		
		]
	  }
	`
	router := mux.NewRouter()
	testService.backend = backend.New(&backend.Builder{
		Config:               configurationJSON,
		DB:                   db,
		Router:               router,
		UpdateSchema:         true,
		AuthorizationEnabled: true,
	})
	creatorClient := client.NewWithRouter(router).WithRole("creator")

	type Release struct {
		ReleaseID   uuid.UUID `json:"release_id"`
		DownloadURL string    `json:"companion_download_url"` // This field is expected to NEVER be populated
		UploadURL   string    `json:"companion_upload_url"`   // This field is expected to NEVER be populated
		Etag        string    `json:"companion_etag"`         // This field is expected to NEVER be populated
	}
	type Artefact struct {
		ArtefactID        uuid.UUID `json:"artefact_id"`
		DownloadURL       string    `json:"companion_download_url"`
		UploadURL         string    `json:"companion_upload_url"`
		DownloadURLExpiry time.Time `json:"companion_download_url_expiry"`
		UploadURLExpiry   time.Time `json:"companion_upload_url_expiry"`
		Etag              string    `json:"companion_etag"`
	}

	// First we create a Release and a Artefacts
	release := Release{}
	_, err := creatorClient.RawPost("/releases", &release, &release)
	if err != nil {
		t.Fatal(err)
	}
	if release.UploadURL != "" {
		t.Fatalf("Expecting %v, got '%v'", "nothing", release.UploadURL)
	}
	if release.DownloadURL != "" {
		t.Fatalf("Expecting %v, got '%v'", "nothing", release.DownloadURL)
	}
	if release.Etag != "" {
		t.Fatalf("Expecting %v, got '%v'", "nothing", release.Etag)
	}

	artefact1 := Artefact{}
	artefact2 := Artefact{}
	releaseArtefactsString := "/releases/" + release.ReleaseID.String() + "/artefacts"
	_, err = creatorClient.RawPost(releaseArtefactsString, &artefact1, &artefact1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = creatorClient.RawPut(releaseArtefactsString, &artefact2, &artefact2)
	if err != nil {
		t.Fatal(err)
	}

	for n, a := range []Artefact{artefact1, artefact2} {

		if a.UploadURL == "" {
			t.Fatalf("Expecting %v, got '%v'", "some url", a.UploadURL)
		}
		if a.DownloadURL != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.DownloadURL)
		}

		if a.UploadURLExpiry.After(time.Now().Add(time.Minute * 5)) {
			t.Fatalf("Expecting %v, got '%v'", "some time in the future", a.UploadURLExpiry)
		}

		resp, err := http.Post(a.UploadURL, "", strings.NewReader("some data "+strconv.Itoa(n)))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode > 299 {
			t.Fatalf("Expecting %v, got '%v'", "below 299", resp.StatusCode)
		}
	}

	readerClient := client.NewWithRouter(router).WithRole("reader")

	// Check that reader role does not allow to create an artefact
	var artefacts []Artefact
	status, err := readerClient.RawPost(releaseArtefactsString, Artefact{}, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got '%v'", status)
	}

	// We check that the default list operation does not return downlod urls
	_, err = readerClient.RawGet(releaseArtefactsString, &artefacts)
	if err != nil {
		t.Fatal(err)
	}
	if len(artefacts) != 2 {
		t.Fatalf("Expecting %v, got '%v'", 2, len(artefacts))
	}

	for _, a := range artefacts {
		if a.UploadURL != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.UploadURL)
		}
		if a.DownloadURL != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.DownloadURL)
		}
		if a.Etag != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.Etag)
		}

	}

	// We check that the list operation returns downlod urls if with_companion_urls=true
	artefacts = []Artefact{}
	status, err = readerClient.RawGet(releaseArtefactsString+"?with_companion_urls=true", &artefacts)
	if err != nil {
		t.Fatal(err)
	}
	if len(artefacts) != 2 {
		t.Fatalf("Expecting %v, got '%v'", 2, len(artefacts))
	}

	for n, a := range artefacts {
		if a.UploadURL != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.UploadURL)
		}
		if a.DownloadURL == "" {
			t.Fatalf("Expecting %v, got '%v'", "some url", a.DownloadURL)
		}
		if a.DownloadURLExpiry.After(time.Now().Add(time.Minute * 5)) {
			t.Fatalf("Expecting %v, got '%v'", "some time in the future", artefact2.DownloadURLExpiry)
		}
		resp, err := http.Get(a.DownloadURL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode > 299 {
			t.Fatalf("Expecting %v, got '%v'", "below 299", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		expected := "some data " + strconv.Itoa(n)
		if string(body) != expected {
			t.Fatalf("Expecting %v, got '%v'", expected, string(body))
		}
	}

	// We check that if we upload new content, etag changes
	aOld := Artefact{}
	_, err = readerClient.RawGet(releaseArtefactsString+artefact1.ArtefactID.String(), &aOld)
	if err != nil {
		t.Fatal(err)
	}
	if aOld.Etag == "" {
		t.Fatalf("Expecting %v, got '%v'", "an etag", aOld.Etag)
	}

	aPut := Artefact{}
	_, err = creatorClient.RawPut(releaseArtefactsString, &aPut, &aPut)
	if err != nil {
		t.Fatal(err)
	}
	if aPut.UploadURL == "" {
		t.Fatalf("Expecting %v, got '%v'", "some url", aPut.UploadURL)
	}

	resp, err := http.Post(aPut.UploadURL, "", strings.NewReader("some new data "))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode > 299 {
		t.Fatalf("Expecting %v, got '%v'", "below 299", resp.StatusCode)
	}
	aNew := Artefact{}
	_, err = readerClient.RawGet(releaseArtefactsString+artefact1.ArtefactID.String(), &aNew)
	if err != nil {
		t.Fatal(err)
	}
	if aNew.Etag == "" || aNew.Etag == aOld.Etag {
		t.Fatalf("Expecting %v, got '%v'", "a new etag", aNew.Etag)
	}
}
