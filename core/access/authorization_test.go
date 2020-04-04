package access

import (
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/relabs-tech/backends/core"
)

func TestMain(m *testing.M) {
	m.Run()
}

func TestAuthorization_Admin(t *testing.T) {

	auth := &Authorization{
		Roles: []string{"admin"},
	}
	resources := []string{"fleet", "user"}

	if !auth.IsAuthorized(resources, core.OperationCreate, QualifierOne, nil, nil) {
		t.Fatal("admin not authorized")
	}
}

func TestAuthorization_Public(t *testing.T) {

	auth := &Authorization{
		Roles: []string{"someone"},
	}
	resources := []string{"fleet", "user"}
	permissions := map[string][]string{
		"public": []string{"read:all/all"},
	}

	if auth.IsAuthorized(resources, core.OperationCreate, QualifierOne, nil, permissions) {
		t.Fatal("public should not create")
	}
	if !auth.IsAuthorized(resources, core.OperationRead, QualifierOne, nil, permissions) {
		t.Fatal("public not authorized for read")
	}

	// now try without any authorization, this should also work
	auth = nil
	if auth.IsAuthorized(resources, core.OperationCreate, QualifierOne, nil, permissions) {
		t.Fatal("public should not create")
	}
	if !auth.IsAuthorized(resources, core.OperationRead, QualifierOne, nil, permissions) {
		t.Fatal("public not authorized for read")
	}

}

func TestAuthorization_Everybody(t *testing.T) {

	auth := &Authorization{
		Roles: []string{"someone"},
	}
	resources := []string{"fleet", "user"}
	permissions := map[string][]string{
		"everybody": []string{"read:all/all"},
	}

	if auth.IsAuthorized(resources, core.OperationCreate, QualifierAll, nil, permissions) {
		t.Fatal("everybody should not create")
	}
	if !auth.IsAuthorized(resources, core.OperationRead, QualifierAll, nil, permissions) {
		t.Fatal("everybody not authorized for read")
	}

	// now try without any authorization, this should not work
	auth = nil
	if auth.IsAuthorized(resources, core.OperationCreate, QualifierAll, nil, permissions) {
		t.Fatal("public should not create")
	}
	if auth.IsAuthorized(resources, core.OperationRead, QualifierAll, nil, permissions) {
		t.Fatal("public should not be authorized for read")
	}

}

func TestAuthorization_One(t *testing.T) {

	userID := uuid.New()

	auth := &Authorization{
		Roles: []string{"user"},
		Resources: map[string]uuid.UUID{
			"user_id": userID,
		},
	}

	resources := []string{"fleet", "user"}
	permissions := map[string][]string{
		"user": []string{"read:all/one"},
	}

	params := map[string]string{
		"user_id": userID.String(),
	}

	if auth.IsAuthorized(resources, core.OperationUpdate, QualifierOne, params, permissions) {
		t.Fatal("user should not update")
	}
	if !auth.IsAuthorized(resources, core.OperationRead, QualifierOne, params, permissions) {
		t.Fatal("user not authorized for read")
	}

	// now try with another user, this should fail
	userID = uuid.New()

	params = map[string]string{
		"user_id": userID.String(),
	}

	if auth.IsAuthorized(resources, core.OperationRead, QualifierOne, params, permissions) {
		t.Fatal("this user should not be authorized for read")
	}

}

func TestAuthorization_ParentOne(t *testing.T) {

	fleetID := uuid.New()
	userID := uuid.New()

	auth := &Authorization{
		Roles: []string{"fleetadmin"},
		Resources: map[string]uuid.UUID{
			"fleet_id": fleetID,
		},
	}

	resources := []string{"fleet", "user"}
	permissions := map[string][]string{
		"fleetadmin": []string{"read:one/all"},
	}

	params := map[string]string{
		"fleet_id": fleetID.String(),
		"user_id":  userID.String(),
	}

	if auth.IsAuthorized(resources, core.OperationUpdate, QualifierOne, params, permissions) {
		t.Fatal("fleetadmin should not update")
	}
	if !auth.IsAuthorized(resources, core.OperationRead, QualifierOne, params, permissions) {
		t.Fatal("fleetadmin not authorized for read one")
	}
	if !auth.IsAuthorized(resources, core.OperationRead, QualifierAll, params, permissions) {
		t.Fatal("fleetadmin not authorized for read all")
	}

	// now try with a different fleet ID, this should fail
	fleetID = uuid.New()
	auth = &Authorization{
		Roles: []string{"fleetadmin"},
		Resources: map[string]uuid.UUID{
			"fleet_id": fleetID,
		},
	}

	if auth.IsAuthorized(resources, core.OperationRead, QualifierOne, params, permissions) {
		t.Fatal("this fleetadmin should not be authorized for read")
	}

	// now try with no fleet ID, this should also fail
	auth = &Authorization{
		Roles: []string{"fleetadmin"},
	}

	if auth.IsAuthorized(resources, core.OperationRead, QualifierOne, params, permissions) {
		t.Fatal("this fleetadmin should not be authorized for read")
	}

}
