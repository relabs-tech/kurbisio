// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/joeshaw/envdecode"
	_ "github.com/lib/pq"

	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/relabs-tech/kurbisio/core/csql"
)

func TestRelationDirectional(t *testing.T) {

	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_relation_directional_unit_test_")
	defer db.Close()
	db.ClearSchema()

	var configurationJSON string = `{
		"collections": [			
	    	{
				"resource": "a"
		  	},
		  	{
				"resource": "b"
		  	}
		],
		"relations": [
			{
				"resource": "a_b_relation",
				"left": "a",
				"right": "b",
				"permits": [
					{
						"role": "role1",
						"operations": [
							"read",
							"create",
							"update",							
							"list",
							"delete"
						]
					},
					{
						"role": "role2",
						"operations": [
							"read",										
							"list",
							"delete"
						],
						"selectors": [
							"a"
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
		LogLevel:             "debug",
	})

	adminClient := client.NewWithRouter(router).WithAdminAuthorization()

	type A struct {
		AID       uuid.UUID `json:"a_id"`
		Timestamp time.Time `json:"timestamp,omitempty"`
	}
	type B struct {
		BID       uuid.UUID `json:"b_id"`
		Timestamp time.Time `json:"timestamp,omitempty"`
	}

	type MyRelation struct {
		AID       uuid.UUID `json:"a_id"`
		BID       uuid.UUID `json:"b_id"`
		Timestamp time.Time `json:"timestamp,omitempty"`
		Index     int       `json:"index,omitempty"`
	}

	// First we create an two As and a B
	a := A{AID: uuid.New()}
	_, err := adminClient.RawPut("/as", &a, &a)
	if err != nil {
		t.Fatal(err)
	}
	a2 := A{AID: uuid.New()}
	_, err = adminClient.RawPut("/as", &a2, &a2)
	if err != nil {
		t.Fatal(err)
	}
	b := B{BID: uuid.New()}
	_, err = adminClient.RawPut("/bs", &b, nil)
	if err != nil {
		t.Fatal(err)
	}

	role1Client := client.NewWithRouter(router).WithRole("role1")
	role2Client := client.NewWithRouter(router).WithRoleAndSelector("role2", "a", a.AID)

	// Check that role2 does not allow to create a relation b/a (role2 lacks create on permits)
	status, _ := role2Client.RawPut(fmt.Sprintf("/a_b_relations/%v:%v", a.AID, b.BID), nil, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}

	// Then we create the relation a/b
	_, err = role1Client.RawPut(fmt.Sprintf("/a_b_relations/%v:%v", a.AID, b.BID), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Then we create the relation a2/b
	_, err = role1Client.RawPut(fmt.Sprintf("/a_b_relations/%v:%v", a2.AID, b.BID), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// We verify that role2 cannot list relations ?/b
	a_b_relations := []MyRelation{}
	status, _ = role2Client.RawGet(fmt.Sprintf("/a_b_relations?right=%v", b.BID), &a_b_relations)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}

	// We verify that role1 can list all relations ?/b
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations?right=%v", b.BID), &a_b_relations)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}
	if len(a_b_relations) != 2 {
		t.Fatalf("Expecting two relations, got %d", len(a_b_relations))
	}

	// We verify that role2 cannot get the relations a2/b, but only a/b
	var a_b_relation MyRelation
	status, _ = role2Client.RawGet(fmt.Sprintf("/a_b_relations/%v:%v", a2.AID, b.BID), &a_b_relation)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}

	status, _ = role2Client.RawGet(fmt.Sprintf("/a_b_relations/%v:%v", a.AID, b.BID), &a_b_relation)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}
	if a_b_relation.AID != a.AID {
		t.Fatalf("Expecting AID=%v, got %v", a.AID, a_b_relation.AID)
	}
	if a_b_relation.BID != b.BID {
		t.Fatalf("Expecting BID=%v, got %v", b.BID, a_b_relation.BID)
	}

	// We verify that role2 cannot list the relation a2/?, but only a/?
	a_b_relations = []MyRelation{}
	status, _ = role2Client.RawGet(fmt.Sprintf("/a_b_relations?left=%v", a2.AID), &a_b_relations)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}
	a_b_relations = []MyRelation{}
	status, _ = role2Client.RawGet(fmt.Sprintf("/a_b_relations?left=%v", a.AID), &a_b_relations)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}

	if len(a_b_relations) != 1 {
		t.Fatalf("Expecting one relation, got %d", len(a_b_relations))
	}
	if a_b_relations[0].AID != a.AID {
		t.Fatalf("Expecting AID=%v, got %v", a.AID, a_b_relations[0].AID)
	}
	if a_b_relations[0].BID != b.BID {
		t.Fatalf("Expecting BID=%v, got %v", b.BID, a_b_relations[0].BID)
	}

	// We verify that role1 can list the relation in both directions
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations?left=%v", a2.AID), nil)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations?left=%v", a.AID), nil)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}

	// We verify that role2 can read relation a/b but not a2/b
	myrelation := MyRelation{}
	status, _ = role2Client.RawGet(fmt.Sprintf("/a_b_relations/%v:%v", a.AID, b.BID), &myrelation)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}
	if myrelation.AID != a.AID {
		t.Fatalf("Expecting AID=%v, got %v", a.AID, myrelation.AID)
	}
	if myrelation.BID != b.BID {
		t.Fatalf("Expecting BID=%v, got %v", b.BID, myrelation.BID)
	}

	status, _ = role2Client.RawGet(fmt.Sprintf("/a_b_relations/%v:%v", a2.AID, b.BID), nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}

	// We verify that role1 can read relation a/b and a2/b
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations/%v:%v", a.AID, b.BID), &myrelation)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}
	if myrelation.AID != a.AID {
		t.Fatalf("Expecting AID=%v, got %v", a.AID, myrelation.AID)
	}
	if myrelation.BID != b.BID {
		t.Fatalf("Expecting BID=%v, got %v", b.BID, myrelation.BID)
	}
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations/%v:%v", a2.AID, b.BID), &myrelation)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}
	if myrelation.AID != a2.AID {
		t.Fatalf("Expecting AID=%v, got %v", a2.AID, myrelation.AID)
	}
	if myrelation.BID != b.BID {
		t.Fatalf("Expecting BID=%v, got %v", b.BID, myrelation.BID)
	}

	// We verify that role1 can delete the relation a/b
	status, _ = role1Client.RawDelete(fmt.Sprintf("/a_b_relations/%v:%v", a.AID, b.BID))
	if status != http.StatusNoContent {
		t.Fatalf("Expecting no content, got %v", status)
	}

	// We verify that relation a/b is gone
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations/%v:%v", a.AID, b.BID), nil)
	if status != http.StatusNotFound {
		t.Fatalf("Expecting not found, got %v", status)
	}

	// We verify that relation a2/b is still there
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations/%v:%v", a2.AID, b.BID), &myrelation)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}

	// Create 1000 b objects
	startTime := time.Now().Add(-time.Minute * 1000).UTC().Truncate(time.Microsecond)
	for i := 0; i < 1000; i++ {
		var b B
		status, err = adminClient.RawPost("/bs", b, &b)
		if status != http.StatusCreated {
			t.Fatalf("Expecting created, got %v (%v)", status, err)
		}
		// and create a relation a2/b for each
		relation := MyRelation{
			AID:       a.AID,
			BID:       b.BID,
			Timestamp: startTime.Add(time.Duration(i) * time.Minute).UTC(),
			Index:     i,
		}
		status, err = role1Client.RawPut("/a_b_relations", &relation, nil)
		if status != http.StatusCreated {
			t.Fatalf("Expecting created, got %v (%v)", status, err)
		}
	}

	// Verify that we can list all relations a2/? in the right order with cursor pagination, defaulting to a limit of 100 items per page
	var allRelations []MyRelation

	for page := role1Client.Collection("a_b_relation").WithParameter("order", "asc").WithLeft(a.AID).FirstPage(); page.HasData(); page = page.Next() {
		var onePage []MyRelation
		_, err := page.Get(&onePage)
		if err != nil {
			t.Fatal(err)
		}
		if len(onePage) != 100 {
			t.Fatalf("Expecting 100 items, got %d", len(onePage))
		}
		allRelations = append(allRelations, onePage...)
	}

	if len(allRelations) != 1000 {
		t.Fatalf("Expecting 1000 relations, got %d", len(allRelations))
	}
	for i, relation := range allRelations {
		if relation.AID != a.AID {
			t.Fatalf("Expecting AID=%v, got %v at index %d", a.AID, relation.AID, i)
		}
		if relation.BID == uuid.Nil {
			t.Fatalf("Expecting valid BID, got nil at index %d", i)
		}
		if relation.Index != i {
			t.Fatalf("Expecting Index=%d, got %d at index %d", i, relation.Index, i)
		}
		expectedTimestamp := startTime.Add(time.Duration(i) * time.Minute).UTC()
		if !relation.Timestamp.Equal(expectedTimestamp) {
			t.Fatalf("Expecting Timestamp=%v, got %v at index %d", expectedTimestamp, relation.Timestamp, i)
		}
	}

	// Verify that deleting a specific b deletes all relations to b
	status, _ = adminClient.RawDelete(fmt.Sprintf("/bs/%v", allRelations[0].BID))
	if status != http.StatusNoContent {
		t.Fatalf("Expecting no content, got %v", status)
	}
	// Verify that all relations to b are gone
	relations := []MyRelation{}
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations?right=%v", allRelations[0].BID), &relations)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}
	if len(relations) != 0 {
		t.Fatalf("Expecting no relations, got %d", len(relations))
	}

	// Verify that deleting a does delete all relations from a
	status, _ = adminClient.RawDelete(fmt.Sprintf("/as/%v", a.AID))
	if status != http.StatusNoContent {
		t.Fatalf("Expecting no content, got %v", status)
	}
	// Verify that all relations from a are gone
	relations = []MyRelation{}
	status, _ = role1Client.RawGet(fmt.Sprintf("/a_b_relations?left=%v", a.AID), &relations)
	if status != http.StatusOK {
		t.Fatalf("Expecting access, got %v", status)
	}
	if len(relations) != 0 {
		t.Fatalf("Expecting no relations, got %d", len(relations))
	}

}

func TestRelationNonDirectional(t *testing.T) {

	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_relation_non_directional_unit_test_")
	defer db.Close()
	db.ClearSchema()

	var configurationJSON string = `{
		"collections": [			
	    	{
				"resource": "a"
		  	}
		],
		"relations": [
			{
				"resource": "a_a_relation",
				"left": "a",
				"right": "a",
				"non_directional": true
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
		LogLevel:             "debug",
	})

	adminClient := client.NewWithRouter(router).WithAdminAuthorization()

	type A struct {
		AID uuid.UUID `json:"a_id"`
	}

	type MyRelation struct {
		LeftAID  uuid.UUID `json:"left_a_id"`
		RightAID uuid.UUID `json:"right_a_id"`
		Text     string    `json:"text,omitempty"`
	}

	// First we create an a
	a := A{AID: uuid.New()}
	_, err := adminClient.RawPut("/as", &a, &a)
	if err != nil {
		t.Fatal(err)
	}

	// Then we create 10 new as, and a relation between a and each of them
	for i := 0; i < 10; i++ {
		var a2 A
		status, err := adminClient.RawPost("/as", a2, &a2)
		if status != http.StatusCreated {
			t.Fatalf("Expecting created, got %v (%v)", status, err)
		}
		// and create a relation a/a2 for each
		relation := MyRelation{
			LeftAID:  a.AID,
			RightAID: a2.AID,
		}
		status, err = adminClient.RawPut("/a_a_relations", &relation, nil)
		if status != http.StatusCreated {
			t.Fatalf("Expecting created, got %v (%v)", status, err)
		}
	}

	// Then we again create 10 new as, and a relation between each of them and a
	for i := 0; i < 10; i++ {
		var a2 A
		status, err := adminClient.RawPost("/as", a2, &a2)
		if status != http.StatusCreated {
			t.Fatalf("Expecting created, got %v (%v)", status, err)
		}
		// and create a relation a/a2 for each
		relation := MyRelation{
			LeftAID:  a2.AID,
			RightAID: a.AID,
		}
		status, err = adminClient.RawPut("/a_a_relations", &relation, nil)
		if status != http.StatusCreated {
			t.Fatalf("Expecting created, got %v (%v)", status, err)
		}
	}

	var relations []MyRelation
	_, err = adminClient.RawGet("/a_a_relations?either="+a.AID.String(), &relations)
	if err != nil {
		t.Fatal(err)
	}
	if len(relations) != 20 {
		t.Fatalf("Expecting 20 relations, got %d", len(relations))
	}

	var leftRelations []MyRelation
	_, err = adminClient.RawGet("/a_a_relations?filter=left_a_id="+a.AID.String(), &leftRelations)
	if err != nil {
		t.Fatal(err)
	}
	var rightRelations []MyRelation
	_, err = adminClient.RawGet("/a_a_relations?filter=right_a_id="+a.AID.String(), &rightRelations)
	if err != nil {
		t.Fatal(err)
	}
	if len(leftRelations)+len(rightRelations) != 20 {
		t.Fatalf("Expecting 20 relations, got %d+%d", len(leftRelations), len(rightRelations))
	}

	someAID := relations[3].LeftAID
	if someAID == a.AID {
		someAID = relations[3].RightAID
	}

	// Verify that someAID is different from a.AID
	if someAID == a.AID {
		t.Fatalf("someAID should not be equal to a.AID, got the same")
	}

	// We verify that someAID has one relation
	relations = []MyRelation{}
	_, err = adminClient.RawGet("/a_a_relations?either="+someAID.String(), &relations)
	if err != nil {
		t.Fatal(err)
	}
	if len(relations) != 1 {
		t.Fatalf("Expecting 1 relation, got %d", len(relations))
	}

	// We verify that we can clear relations with a filter
	_, err = adminClient.RawDelete("/a_a_relations?either=" + someAID.String())
	if err != nil {
		t.Fatal(err)
	}

	// We verify that all relations with the given a are gone
	relations = []MyRelation{}
	_, err = adminClient.RawGet("/a_a_relations?either="+someAID.String(), &relations)
	if err != nil {
		t.Fatal(err)
	}
	if len(relations) != 0 {
		t.Fatalf("Expecting no relations, got %d", len(relations))
	}

	// We verify that we now have 1 relation less
	relations = []MyRelation{}
	_, err = adminClient.RawGet("/a_a_relations?either="+a.AID.String(), &relations)
	if err != nil {
		t.Fatal(err)
	}
	if len(relations) != 19 {
		t.Fatalf("Expecting 19 relations, got %d", len(relations))
	}

	// One more test that non-directional relations are indeed non-directional

	// verify that a and someAID have no relation
	relation := MyRelation{}
	status, _ := adminClient.RawGet(fmt.Sprintf("/a_a_relations/%v:%v", a.AID, someAID), &relation)
	if status != http.StatusNotFound {
		t.Fatalf("Expecting not found, got %v", status)
	}
	// verify that someAID and a have no relation
	relation = MyRelation{}
	status, _ = adminClient.RawGet(fmt.Sprintf("/a_a_relations/%v:%v", someAID, a.AID), &relation)
	if status != http.StatusNotFound {
		t.Fatalf("Expecting not found, got %v", status)
	}

	// create a new relation between a and someAID
	status, err = adminClient.RawPut("/a_a_relations", &MyRelation{
		LeftAID:  a.AID,
		RightAID: someAID,
		Text:     "magic token",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusCreated {
		t.Fatalf("Expecting created, got %v", status)
	}

	// read it back with left and right as they are
	relation = MyRelation{}
	_, err = adminClient.RawGet(fmt.Sprintf("/a_a_relations/%v:%v", a.AID, someAID), &relation)
	if err != nil {
		t.Fatal(err)
	}
	if relation.Text != "magic token" {
		t.Fatalf("Expecting magic token, got %s", relation.Text)
	}

	// read it back with left and right swapped
	relation = MyRelation{}
	_, err = adminClient.RawGet(fmt.Sprintf("/a_a_relations/%v:%v", someAID, a.AID), &relation)
	if err != nil {
		t.Fatal(err)
	}
	if relation.Text != "magic token" {
		t.Fatalf("Expecting magic token, got %s", relation.Text)
	}

	// update it with left and right swapped, should not create a new one
	// create a new relation between a and someAID
	status, err = adminClient.RawPut("/a_a_relations", &MyRelation{
		LeftAID:  a.AID,
		RightAID: someAID,
		Text:     "new magic token",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("Expecting OK, got %v", status)
	}

	// read it back and check that the token was updated
	relation = MyRelation{}
	_, err = adminClient.RawGet(fmt.Sprintf("/a_a_relations/%v:%v", a.AID, someAID), &relation)
	if err != nil {
		t.Fatal(err)
	}
	if relation.Text != "new magic token" {
		t.Fatalf("Expecting new magic token, got %s", relation.Text)
	}

}

// use POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
// and POSTGRES_PASSWORD="docker"
type TestService struct {
	Postgres         string `env:"POSTGRES,required" description:"the connection string for the Postgres DB without password"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,optional" description:"password to the Postgres DB"`
	backend          *backend.Backend
	client           client.Client
	clientNoAuth     client.Client
	Db               *csql.DB
	Router           *mux.Router
}

var testService TestService
