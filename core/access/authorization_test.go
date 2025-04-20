// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package access

import (
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/relabs-tech/kurbisio/core"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestAuthorization_Admin(t *testing.T) {

	auth := &Authorization{
		Roles: []string{"admin"},
	}

	if !auth.IsAuthorized(core.OperationCreate, nil, nil) {
		t.Fatal("admin not authorized")
	}
}

func TestAuthorization_AdminViewer(t *testing.T) {
	auth := &Authorization{
		Roles: []string{"admin viewer"},
	}

	if auth.IsAuthorized(core.OperationCreate, nil, nil) {
		t.Fatal("admin viewer authorized to create")
	}
	if auth.IsAuthorized(core.OperationDelete, nil, nil) {
		t.Fatal("admin viewer authorized to create")
	}
	if auth.IsAuthorized(core.OperationUpdate, nil, nil) {
		t.Fatal("admin viewer authorized to create")
	}
	if !auth.IsAuthorized(core.OperationList, nil, nil) {
		t.Fatal("admin viewer not authorized to create")
	}
	if !auth.IsAuthorized(core.OperationRead, nil, nil) {
		t.Fatal("admin viewer not authorized to create")
	}
}

func TestAuthorization_Public(t *testing.T) {

	auth := &Authorization{
		Roles: []string{"someone"},
	}
	permit := Permit{
		Role:       "public",
		Operations: []core.Operation{"read"},
	}
	permits := []Permit{permit}

	if auth.IsAuthorized(core.OperationCreate, nil, permits) {
		t.Fatal("public should not create")
	}
	if !auth.IsAuthorized(core.OperationRead, nil, permits) {
		t.Fatal("public not authorized for read")
	}

	// now try without any authorization, this should also work
	auth = nil
	if auth.IsAuthorized(core.OperationCreate, nil, permits) {
		t.Fatal("public should not create")
	}
	if !auth.IsAuthorized(core.OperationRead, nil, permits) {
		t.Fatal("public not authorized for read")
	}

}

func TestAuthorization_Everybody(t *testing.T) {

	auth := &Authorization{
		Roles: []string{"someone"},
	}
	permit := Permit{
		Role:       "everybody",
		Operations: []core.Operation{"read"},
	}
	permits := []Permit{permit}

	if auth.IsAuthorized(core.OperationCreate, nil, permits) {
		t.Fatal("everybody should not create")
	}
	if !auth.IsAuthorized(core.OperationRead, nil, permits) {
		t.Fatal("everybody not authorized for read")
	}

	// now try without any authorization, this should not work
	auth = nil
	if auth.IsAuthorized(core.OperationCreate, nil, permits) {
		t.Fatal("public should not create")
	}
	if auth.IsAuthorized(core.OperationRead, nil, permits) {
		t.Fatal("public should not be authorized for read")
	}

}

func TestAuthorization_Selector(t *testing.T) {

	userID := uuid.New()

	auth := &Authorization{
		Roles: []string{"userrole"},
		Selectors: map[string]string{
			"user_id": userID.String(),
		},
	}

	permit := Permit{
		Role:       "userrole",
		Operations: []core.Operation{"read"},
		Selectors:  []string{"user"},
	}
	permits := []Permit{permit}

	params := map[string]string{
		"user_id": userID.String(),
	}

	if auth.IsAuthorized(core.OperationUpdate, params, permits) {
		t.Fatal("user should not update")
	}
	if !auth.IsAuthorized(core.OperationRead, params, permits) {
		t.Fatal("user not authorized for read")
	}

	// now try with another user, this should fail
	userID = uuid.New()

	params = map[string]string{
		"user_id": userID.String(),
	}

	if auth.IsAuthorized(core.OperationRead, params, permits) {
		t.Fatal("this user should not be authorized for read")
	}

}

func TestAuthorization_ParentSelector(t *testing.T) {

	fleetID := uuid.New()
	userID := uuid.New()

	auth := &Authorization{
		Roles: []string{"fleetadmin"},
		Selectors: map[string]string{
			"fleet_id": fleetID.String(),
		},
	}

	permit := Permit{
		Role:       "fleetadmin",
		Operations: []core.Operation{"read"},
		Selectors:  []string{"fleet"},
	}
	permits := []Permit{permit}

	params := map[string]string{
		"fleet_id": fleetID.String(),
		"user_id":  userID.String(),
	}

	if auth.IsAuthorized(core.OperationUpdate, params, permits) {
		t.Fatal("fleetadmin should not update")
	}
	if !auth.IsAuthorized(core.OperationRead, params, permits) {
		t.Fatal("fleetadmin not authorized for read on single user")
	}
	if !auth.IsAuthorized(core.OperationRead, params, permits) {
		t.Fatal("fleetadmin not authorized for read all users")
	}

	// now try with the admin of a different fleet, this should fail
	fleetID = uuid.New()
	auth = &Authorization{
		Roles: []string{"fleetadmin"},
		Selectors: map[string]string{
			"fleet_id": fleetID.String(),
		},
	}

	if auth.IsAuthorized(core.OperationRead, params, permits) {
		t.Fatal("this fleetadmin should not be authorized for read")
	}

	// now try with a fleetadmin without fleet, this should also fail
	auth = &Authorization{
		Roles: []string{"fleetadmin"},
	}

	if auth.IsAuthorized(core.OperationRead, params, permits) {
		t.Fatal("this fleetadmin should not be authorized for read")
	}

}
