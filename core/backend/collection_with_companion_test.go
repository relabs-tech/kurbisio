// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/backend/kss"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/relabs-tech/kurbisio/core/csql"
)

var s3Credentials kss.S3Credentials
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
		  },
		  {
			"resource":"c_with_companion",
			"with_companion_file": true
		  }	 		
  
	  ]
  }
`

type CWithKss struct {
	CID         uuid.UUID `json:"c_with_companion_id"`
	DownloadURL string    `json:"companion_download_url"` // This field is expected to NEVER be populated
	UploadURL   string    `json:"companion_upload_url"`   // This field is expected to NEVER be populated
}
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

func TestCompanion_LocalFilesystem(t *testing.T) {
	testCompanion(t, kss.DriverTypeLocal)
}

func testCompanion(t *testing.T, kssDrv kss.DriverType) {

	var kssConfiguration kss.Configuration
	dir := t.TempDir()
	router := mux.NewRouter()
	creatorClient := client.NewWithRouter(router).WithRole("creator")
	var externalClient client.Client
	if kssDrv == kss.DriverTypeLocal {
		externalClient = creatorClient
		kssConfiguration = kss.Configuration{
			DriverType: kssDrv,
			LocalConfiguration: &kss.LocalConfiguration{
				KeyPrefix: dir,
				PublicURL: "",
			},
		}
	} else if kssDrv == kss.DriverTypeAWSS3 {
		kssConfiguration = kss.Configuration{
			DriverType: kssDrv,
			S3Configuration: &kss.S3Configuration{
				AccessID:      s3Credentials.AccessID,
				AccessKey:     s3Credentials.AccessKey,
				AWSBucketName: "kss-test",
				AWSRegion:     "eu-central-1",
				KeyPrefix:     t.Name() + time.Now().Format("2006-01-0215.04.05.9.00"),
			},
		}
		externalClient = client.NewWithURL("")

	}

	var testService TestService

	// Create a relation and verifies that the relation can be listed
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_companion_unit_test_"+t.Name())
	defer db.Close()
	db.ClearSchema()

	testService.backend = backend.New(&backend.Builder{
		Config:               configurationCompanionJSON,
		DB:                   db,
		Router:               router,
		UpdateSchema:         true,
		AuthorizationEnabled: true,
		KssConfiguration:     kssConfiguration,
	})

	// First we create a Release and Artefacts
	release, b, _, err := createReleaseAndArtefacts(3, creatorClient, externalClient)
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

	// We check that individual Get return the download URL
	for n, a := range artefacts {
		var aGet Artefact
		_, err = readerClient.RawGet(releaseArtefactsString+"/"+a.ArtefactID.String(), &aGet)
		if err != nil {
			t.Fatal(err)
		}

		if aGet.UploadURL != "" {
			t.Fatalf("Expecting %v, got '%v'", "nothing", a.UploadURL)
		}
		if aGet.DownloadURL == "" {
			t.Fatalf("Expecting %v, got '%v'", "some url", a.DownloadURL)
		}

		var data []byte
		status, _, err := externalClient.RawGetBlobWithHeader(aGet.DownloadURL, map[string]string{}, &data)
		if err != nil {
			t.Fatal(err)
		}
		if status > 299 {
			t.Fatalf("Expecting '%v', got '%v'", "below 299", status)
		}
		expected := "some data " + strconv.Itoa(len(artefacts)-n-1)
		if string(data) != expected {
			t.Fatalf("Expecting '%v', got '%v'", expected, string(data))
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
		status, _, err := externalClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
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

func TestCompanionPut(t *testing.T) {

	// Test that PUT on an existing property do generate a new upload URL

	var kssConfiguration kss.Configuration
	dir := t.TempDir()

	router := mux.NewRouter()
	creatorClient := client.NewWithRouter(router).WithAdminAuthorization()

	kssConfiguration = kss.Configuration{
		DriverType: kss.DriverTypeLocal,
		LocalConfiguration: &kss.LocalConfiguration{
			KeyPrefix: dir,
			PublicURL: "",
		},
	}

	var testService TestService

	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_companion_unit_test_"+t.Name())
	defer db.Close()
	db.ClearSchema()

	testService.backend = backend.New(&backend.Builder{
		Config:               configurationCompanionJSON,
		DB:                   db,
		Router:               router,
		UpdateSchema:         true,
		AuthorizationEnabled: true,
		KssConfiguration:     kssConfiguration,
	})

	c := CWithKss{CID: uuid.New()}
	_, err := creatorClient.RawPut("/c_with_companions", &c, &c)
	if err != nil {
		t.Fatal(err)
	}

	if c.UploadURL == "" {
		t.Errorf("Expecting UploadURL, got nothing")
	}

	c2 := CWithKss{CID: c.CID}
	_, err = creatorClient.RawPut("/c_with_companions", c2, &c2)
	if err != nil {
		t.Fatal(err)
	}

	if c2.UploadURL == "" {
		t.Errorf("Expecting UploadURL, got nothing")
	}
}

func TestCompanion_Delete_LocalFilesystem(t *testing.T) {
	testCompanion_Delete(t, kss.DriverTypeLocal)
}

func testCompanion_Delete(t *testing.T, kssDrv kss.DriverType) {

	var kssConfiguration kss.Configuration
	dir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	router := mux.NewRouter()
	creatorClient := client.NewWithRouter(router).WithRole("creator")
	var externalClient client.Client
	defer os.RemoveAll(dir) // clean up
	if kssDrv == kss.DriverTypeLocal {
		externalClient = creatorClient
		kssConfiguration = kss.Configuration{
			DriverType: kssDrv,
			LocalConfiguration: &kss.LocalConfiguration{
				KeyPrefix: dir,
				PublicURL: "",
			},
		}
	} else if kssDrv == kss.DriverTypeAWSS3 {
		kssConfiguration = kss.Configuration{
			DriverType: kssDrv,
			S3Configuration: &kss.S3Configuration{
				AccessID:      s3Credentials.AccessID,
				AccessKey:     s3Credentials.AccessKey,
				AWSBucketName: "kss-test",
				AWSRegion:     "eu-central-1",
				KeyPrefix:     t.Name() + time.Now().Format("2006-01-0215.04.05.9.00"),
			},
		}
		externalClient = client.NewWithURL("")

	}

	var testService TestService

	// Create a relation and verifies that the relation can be listed
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_companion_unit_test_"+t.Name())
	defer db.Close()
	db.ClearSchema()

	testService.backend = backend.New(&backend.Builder{
		Config:               configurationCompanionJSON,
		DB:                   db,
		Router:               router,
		UpdateSchema:         true,
		AuthorizationEnabled: true,
		KssConfiguration:     kssConfiguration,
	})

	release, b, artefacts, err := createReleaseAndArtefacts(3, creatorClient, externalClient)
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
	release, b, artefacts, err = createReleaseAndArtefacts(3, creatorClient, externalClient)
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
	release, b, artefacts, err = createReleaseAndArtefacts(3, creatorClient, externalClient)
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
	release1, b1, artefacts1, err := createReleaseAndArtefacts(3, creatorClient, externalClient)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString1 := "/releases/" + release1.ReleaseID.String() + "/bs/" + b1.BID.String() + "/artefacts"

	_, err = readerClient.RawGet(releaseArtefactsString1+"?with_companion_urls=true", &artefacts1)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artefacts1 {
		status, _, err = externalClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status > 299 {
			t.Fatalf("Expecting OK, got %v", status)
		}
	}
	release2, b2, artefacts2, err := createReleaseAndArtefacts(3, creatorClient, externalClient)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString2 := "/releases/" + release2.ReleaseID.String() + "/bs/" + b2.BID.String() + "/artefacts"

	_, err = readerClient.RawGet(releaseArtefactsString2+"?with_companion_urls=true", &artefacts2)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artefacts2 {
		status, _, err = externalClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status > 299 {
			t.Fatalf("Expecting OK, got %v", status)
		}
	}

	_, err = creatorClient.RawDelete("/releases/" + release1.ReleaseID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range artefacts1 {
		status, _, err = externalClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status != http.StatusNotFound {
			t.Fatalf("Expecting deleted file")
		}
	}
	for _, a := range artefacts2 {
		status, _, err = externalClient.RawGetBlobWithHeader(a.DownloadURL, map[string]string{}, &data)
		if status > 299 {
			t.Fatalf("Expecting OK, got %v", status)
		}
	}
}

func createReleaseAndArtefacts(nbOfArtefacts int, cl, externalClient client.Client) (release Release, b B, artefacts []Artefact, err error) {
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
			_, err = cl.RawPut(releaseArtefactsString, &artefacts[n], &artefacts[n])
		}
		if err != nil {
			return
		}

		if artefacts[n].UploadURL == "" {
			err = fmt.Errorf("Expecting %v, got '%v'", "UploadURL", artefacts[n].UploadURL)
			return
		}
		var status int
		status, err = externalClient.RawPut(artefacts[n].UploadURL, []byte("some data "+strconv.Itoa(n)), nil)
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
func TestCompanion_Notifications_LocalFilesystem(t *testing.T) {
	testCompanion_Notifications(t, kss.DriverTypeLocal)
}

func testCompanion_Notifications(t *testing.T, kssDrv kss.DriverType) {
	// Check that we can register to be notified when a file is uploaded to the KSS backend

	var kssConfiguration kss.Configuration

	var externalClient client.Client
	router := mux.NewRouter()
	creatorClient := client.NewWithRouter(router).WithRole("creator")
	if kssDrv == kss.DriverTypeLocal {
		dir, err := os.MkdirTemp("", "test")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir) // clean up
		externalClient = creatorClient
		kssConfiguration = kss.Configuration{
			DriverType: kss.DriverTypeLocal,
			LocalConfiguration: &kss.LocalConfiguration{
				KeyPrefix: dir,
				PublicURL: "",
			},
		}
	} else if kssDrv == kss.DriverTypeAWSS3 {
		kssConfiguration = kss.Configuration{
			DriverType: kssDrv,
			S3Configuration: &kss.S3Configuration{
				AccessID:             s3Credentials.AccessID,
				AccessKey:            s3Credentials.AccessKey,
				AWSBucketName:        "kss-test",
				AWSRegion:            "eu-central-1",
				KeyPrefix:            t.Name() + time.Now().Format("2006-01-0215.04.05.9.00"),
				SQSNotificationQueue: "TestS3BucketNotificationToSQS",
			},
		}
		externalClient = client.NewWithURL("")

	}

	var testService TestService

	// Create a relation and verifies that the relation can be listed
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_companion_unit_test_"+t.Name())
	defer db.Close()
	db.ClearSchema()

	testService.backend = backend.New(&backend.Builder{
		Config:               configurationCompanionJSON,
		DB:                   db,
		Router:               router,
		UpdateSchema:         true,
		AuthorizationEnabled: true,
		KssConfiguration:     kssConfiguration,
	})
	var release Release
	var b B
	_, err := creatorClient.RawPost("/releases", &release, &release)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString := "/releases/" + release.ReleaseID.String()

	_, err = creatorClient.RawPost(releaseArtefactsString+"/bs", &b, &b)
	if err != nil {
		t.Fatal(err)
	}
	releaseArtefactsString += "/bs/" + b.BID.String() + "/artefacts"
	var artefact Artefact
	_, err = creatorClient.RawPost(releaseArtefactsString, &artefact, &artefact)
	if err != nil {
		t.Fatal(err)
	}

	if artefact.UploadURL == "" {
		err = fmt.Errorf("Expecting %v, got '%v'", "UploadURL", artefact.UploadURL)
		t.Fatal(err)
	}
	called := make(chan bool)

	uploadHandler := func(ctx context.Context, n backend.Notification) error {
		called <- true
		return nil
	}
	testService.backend.HandleResourceNotification("release/b/artefact", uploadHandler, core.OperationCompanionUploaded)

	var status int
	status, err = externalClient.RawPut(artefact.UploadURL, []byte("some data"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if status > 299 {
		err = fmt.Errorf("Expecting %v, got '%v'", "below 299", status)
		t.Fatal(err)
	}
	done := make(chan bool)

	go func() {
		for {
			testService.backend.ProcessJobsSync(0)
			time.Sleep(time.Second)
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	select {
	case <-time.After(120 * time.Second):
		done <- true
		t.Fatal("Timeout waiting for notification to be received")
	case <-called:
		done <- true
	}

}
