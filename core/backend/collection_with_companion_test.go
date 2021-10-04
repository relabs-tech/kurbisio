package backend_test

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/backend"
	"github.com/relabs-tech/backends/core/backend/kss"
	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/csql"
)

var configurationCompanionJSON string = `{
	"collections": [			
		{
		  "resource":"release",
		  "permits": [
				{
					"role": "creator",
					"operations": [
						"create",
						"update",
						"delete"
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
			"resource":"release/b",
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
		  "resource":"release/b/artefact",
		  "with_companion_file": true,
		  "permits": [
				{
					"role": "creator",
					"operations": [
						"create",
						"update",
						"delete",
						"clear"
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

type Release struct {
	ReleaseID   uuid.UUID `json:"release_id"`
	DownloadURL string    `json:"companion_download_url"` // This field is expected to NEVER be populated
	UploadURL   string    `json:"companion_upload_url"`   // This field is expected to NEVER be populated
}
type Artefact struct {
	ArtefactID  uuid.UUID `json:"artefact_id"`
	DownloadURL string    `json:"companion_download_url"`
	UploadURL   string    `json:"companion_upload_url"`
}
type B struct {
	BID uuid.UUID `json:"b_id"`
}

func TestCompanion(t *testing.T) {
	var testService TestService

	// Create a relation and verifies that the relation can be listed
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_companion_unit_test_")
	defer db.Close()
	db.ClearSchema()

	dir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	router := mux.NewRouter()
	testService.backend = backend.New(&backend.Builder{
		Config:               configurationCompanionJSON,
		DB:                   db,
		Router:               router,
		UpdateSchema:         true,
		AuthorizationEnabled: true,
		KssConfiguration: kss.Configuration{
			DriverType: kss.DriverTypeLocal,
			LocalConfiguration: &kss.LocalConfiguration{
				BasePath: dir,
			},
		},
	})
	creatorClient := client.NewWithRouter(router).WithRole("creator")

	// First we create a Release and Artefacts
	release, b, _, err := createReleaseAndArtefacts(3, creatorClient)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString := "/releases/" + release.ReleaseID.String()
	releaseArtefactsString += "/bs/" + b.BID.String() + "/artefacts"

	readerClient := client.NewWithRouter(router).WithRole("reader")

	// Check that reader role does not allow to create an artefact
	var artefacts []Artefact
	status, err := readerClient.RawPost(releaseArtefactsString, Artefact{}, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got '%v'", status)
	}

	// We check that the default list operation does not return download urls
	_, err = readerClient.RawGet(releaseArtefactsString, &artefacts)
	if err != nil {
		t.Fatal(err)
	}
	if len(artefacts) != 3 {
		t.Fatalf("Expecting %v, got '%v'", 3, len(artefacts))
	}

	for _, a := range artefacts {
		if a.UploadURL != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.UploadURL)
		}
		if a.DownloadURL != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.DownloadURL)
		}
	}

	// We check that the list operation returns download urls if with_companion_urls=true
	artefacts = []Artefact{}
	status, err = readerClient.RawGet(releaseArtefactsString+"?with_companion_urls=true&order=asc", &artefacts)
	if err != nil {
		t.Fatal(err)
	}
	if len(artefacts) != 3 {
		t.Fatalf("Expecting %v, got '%v'", 3, len(artefacts))
	}

	for n, a := range artefacts {
		if a.UploadURL != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.UploadURL)
		}
		if a.DownloadURL == "" {
			t.Fatalf("Expecting %v, got '%v'", "some url", a.DownloadURL)
		}

		var data []byte
		status, _, err := readerClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if err != nil {
			t.Fatal(err)
		}
		if status > 299 {
			t.Fatalf("Expecting %v, got '%v'", "below 299", status)
		}
		expected := "some data " + strconv.Itoa(n)
		if string(data) != expected {
			t.Fatalf("Expecting %v, got '%v'", expected, string(data))
		}
	}
}

func TestCompanion_Delete(t *testing.T) {
	var testService TestService

	// Create a relation and verifies that the relation can be listed
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_companion_unit_test_delete_")
	defer db.Close()
	db.ClearSchema()

	dir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	router := mux.NewRouter()
	testService.backend = backend.New(&backend.Builder{
		Config:               configurationCompanionJSON,
		DB:                   db,
		Router:               router,
		UpdateSchema:         true,
		AuthorizationEnabled: true,
		KssConfiguration: kss.Configuration{
			DriverType: kss.DriverTypeLocal,
			LocalConfiguration: &kss.LocalConfiguration{
				BasePath: dir,
			},
		},
	})
	creatorClient := client.NewWithRouter(router).WithRole("creator")

	release, b, artefacts, err := createReleaseAndArtefacts(3, creatorClient)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString := "/releases/" + release.ReleaseID.String()
	releaseArtefactsString += "/bs/" + b.BID.String() + "/artefacts"

	readerClient := client.NewWithRouter(router).WithRole("reader")
	_, err = readerClient.RawGet(releaseArtefactsString+"?with_companion_urls=true", &artefacts)
	if err != nil {
		t.Fatal(err)
	}

	// Test delete single element
	key := releaseArtefactsString + "/" + artefacts[0].ArtefactID.String()
	_, err = creatorClient.RawDelete(key)
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	status, _, err := readerClient.RawGetBlobWithHeader(artefacts[0].DownloadURL, map[string]string{}, &data)
	if status != http.StatusNotFound {
		t.Fatalf("Expecting deleted file")
	}

	// Test clear
	_, err = creatorClient.RawDelete(releaseArtefactsString)
	if err != nil {
		t.Fatal(err)
	}
	status, _, err = readerClient.RawGetBlobWithHeader(artefacts[1].DownloadURL, map[string]string{}, &data)
	if status != http.StatusNotFound {
		t.Fatalf("Expecting deleted file")
	}
	status, _, err = readerClient.RawGetBlobWithHeader(artefacts[2].DownloadURL, map[string]string{}, &data)
	if status != http.StatusNotFound {
		t.Fatalf("Expecting deleted file")
	}

	// We delete the release and we check that the underneath files are also deleted
	release, b, artefacts, err = createReleaseAndArtefacts(3, creatorClient)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString = "/releases/" + release.ReleaseID.String()
	artefactsString := releaseArtefactsString + "/bs/" + b.BID.String() + "/artefacts"

	_, err = readerClient.RawGet(artefactsString+"?with_companion_urls=true", &artefacts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = creatorClient.RawDelete("/releases/" + release.ReleaseID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artefacts {
		status, _, err = readerClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status != http.StatusNotFound {
			t.Fatal("Expecting deleted file", a.ArtefactID)
		}
	}

	// We delete the release/bs/all and we check that the underneath files are also deleted
	release, b, artefacts, err = createReleaseAndArtefacts(3, creatorClient)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString = "/releases/" + release.ReleaseID.String() + "/bs/all/artefacts"

	_, err = readerClient.RawGet(releaseArtefactsString+"?with_companion_urls=true", &artefacts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = creatorClient.RawDelete("/releases/" + release.ReleaseID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artefacts {
		status, _, err = readerClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status != http.StatusNotFound {
			t.Fatalf("Expecting deleted file")
		}
	}

	// Create two releases and delete one, the file fromthe remaining one should be untouched
	release1, b1, artefacts1, err := createReleaseAndArtefacts(3, creatorClient)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString1 := "/releases/" + release1.ReleaseID.String() + "/bs/" + b1.BID.String() + "/artefacts"

	_, err = readerClient.RawGet(releaseArtefactsString1+"?with_companion_urls=true", &artefacts1)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artefacts1 {
		status, _, err = readerClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status > 299 {
			t.Fatalf("Expecting OK, got %v", status)
		}
	}
	release2, b2, artefacts2, err := createReleaseAndArtefacts(3, creatorClient)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString2 := "/releases/" + release2.ReleaseID.String() + "/bs/" + b2.BID.String() + "/artefacts"

	_, err = readerClient.RawGet(releaseArtefactsString2+"?with_companion_urls=true", &artefacts2)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artefacts2 {
		status, _, err = readerClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status > 299 {
			t.Fatalf("Expecting OK, got %v", status)
		}
	}

	_, err = creatorClient.RawDelete("/releases/" + release1.ReleaseID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artefacts1 {
		status, _, err = readerClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status != http.StatusNotFound {
			t.Fatalf("Expecting deleted file")
		}
	}
	for _, a := range artefacts2 {
		status, _, err = readerClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status > 299 {
			t.Fatalf("Expecting OK, got %v", status)
		}
	}

}

func createReleaseAndArtefacts(nbOfArtefacts int, cl client.Client) (release Release, b B, artefacts []Artefact, err error) {
	// First we create a Release and a Artefacts
	// We do not expect any upload/download URL here because this is not a resource with a companion file
	_, err = cl.RawPost("/releases", &release, &release)
	if err != nil {
		return
	}
	// We do not expect any upload/download URL here because this is not a resource with a companion file
	if release.UploadURL != "" {
		err = fmt.Errorf("Expecting %v, got '%v'", "nothing", release.UploadURL)
		return
	}
	if release.DownloadURL != "" {
		err = fmt.Errorf("Expecting %v, got '%v'", "nothing", release.DownloadURL)
		return
	}

	releaseArtefactsString := "/releases/" + release.ReleaseID.String()

	_, err = cl.RawPost(releaseArtefactsString+"/bs", &b, &b)
	if err != nil {
		return
	}
	releaseArtefactsString += "/bs/" + b.BID.String() + "/artefacts"

	for n := 0; n < nbOfArtefacts; n++ {

		if n%2 == 0 { // we alternate post and put
			artefacts = append(artefacts, Artefact{})
			_, err = cl.RawPost(releaseArtefactsString, &artefacts[n], &artefacts[n])
		} else {
			artefacts = append(artefacts, Artefact{ArtefactID: uuid.New()})
			_, err = cl.RawPost(releaseArtefactsString, &artefacts[n], &artefacts[n])
		}
		if err != nil {
			return
		}

		if artefacts[n].UploadURL == "" {
			err = fmt.Errorf("Expecting %v, got '%v'", "UploadURL", artefacts[n].UploadURL)
			return
		}
		var status int
		status, err = cl.PostMultipart(artefacts[n].UploadURL, []byte("some data "+strconv.Itoa(n)))
		if err != nil {
			return
		}
		if status > 299 {
			err = fmt.Errorf("Expecting %v, got '%v'", "below 299", status)
			return
		}
	}
	return
}
