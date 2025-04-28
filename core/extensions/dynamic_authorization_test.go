package extensions_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/relabs-tech/kurbisio/core/extensions"
)

// TestDynamicAuthNoShortCut verifies that the extension panics when the target collection is not present
func TestDynamicAuthNoShortCut(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Should have panicked because there is no account shortcut")
		}
	}()

	testService := createTestService(`{"collections": []}`,
		t.Name(), extensions.DynamicAuth{
			TargetCollection: "org",
		})
	defer testService.close()
}

func TestDynamicAuth(t *testing.T) {
	testService := createTestService(`
	{
		"collections": [
			{"resource": "account"},
			{"resource": "user"},
			{"resource": "org",
				"permits": [
    				{
						"role": "org_admin",
						"operations": [
							"read", "update", "create", "list", "delete"
						],
						"selectors": [
							"org"
						]
					},
					{
						"role": "org_participant",
						"operations": [
							"read"
						],
						"selectors": [
							"org"
						]
					}
			    ]
		    },
			{"resource": "org/role",
				"permits": [
    				{
					"role": "org_admin",
					"operations": [
						"read", "update", "create", "list", "delete"
					],
					"selectors": [
						"org"
					]
				    }
				]
			},
			{"resource": "org/team/role",
				"permits": [
    				{
					"role": "org_admin",
					"operations": [
						"read", "update", "create", "list", "delete"
					],
					"selectors": [
						"org"
					]
				    }
				]
			},
			{"resource": "org/team",
				"permits": [
    				{
					"role": "org_admin",
					"operations": [
						"read", "update", "create", "list", "delete"
					],
					"selectors": [
						"org"
					]
				
				},
				{
					"role": "team_admin",
					"operations": [
						"read", "update"
					],
					"selectors": [
						"org"
					]
				}
			    ]
		    }
		]
	}`,
		t.Name(), extensions.DynamicAuth{
			TargetCollection: "org",
			Selector:         "user",
			RolesCollection:  "org/role",
			AllowedRoles:     []string{"org_admin", "org_participant"},
		}, extensions.DynamicAuth{
			TargetCollection: "org/team",
			Selector:         "user",
			RolesCollection:  "org/team/role",
			AllowedRoles:     []string{"team_admin"},
		},
	)
	defer testService.close()

	adminCl := testService.client.WithAdminAuthorization()
	user1ID, user1Cl, err := testService.createAccount()
	if err != nil {
		t.Fatalf("Should have been able to create account: %v", err)
	}
	user2ID, user2Cl, err := testService.createAccount()
	if err != nil {
		t.Fatalf("Should have been able to create account: %v", err)
	}
	status, _ := user1Cl.Collection("org").List(nil)
	if status != 401 {
		t.Fatalf("Should not have been able to list orgs: got status %d", status)
	}
	user3ID, user3Cl, err := testService.createAccount()
	if err != nil {
		t.Fatalf("Should have been able to create account: %v", err)
	}

	type Org struct {
		OrgID uuid.UUID `json:"org_id"`
	}
	type Team struct {
		OrgID  uuid.UUID `json:"org_id"`
		TeamID uuid.UUID `json:"team_id"`
	}
	type Role struct {
		Roles  []string  `json:"roles"`
		UserId uuid.UUID `json:"user_id"`
	}
	org1 := Org{}
	org2 := Org{}
	if _, err = adminCl.Collection("org").Create(org1, &org1); err != nil {
		t.Fatalf("Should have been able to create org: %v", err)
	}
	if _, err = adminCl.Collection("org").Create(org2, &org2); err != nil {
		t.Fatalf("Should have been able to create org: %v", err)
	}

	anOrg := Org{}
	if status, _ := user1Cl.Collection("org").Item(org1.OrgID).Read(&anOrg); status != 401 {
		t.Fatalf("Should not have been able to read the org: got status %d", status)
	}

	if status, _ := user3Cl.Collection("org").Item(org1.OrgID).Read(&anOrg); status != 401 {
		t.Fatalf("Should not have been able to read the org: got status %d", status)
	}

	if _, err = adminCl.Collection("org/role").WithParent(org1.OrgID).Create(
		Role{UserId: user1ID, Roles: []string{"org_admin"}}, nil); err != nil {
		t.Fatalf("Should have been able to create a role: %v", err)
	}
	if _, err = adminCl.Collection("org/role").WithParent(org2.OrgID).Create(
		Role{UserId: user2ID, Roles: []string{"org_admin"}}, nil); err != nil {
		t.Fatalf("Should have been able to create a role: %v", err)
	}
	if _, err = user1Cl.Collection("org/role").WithParent(org1.OrgID).Create(
		Role{UserId: user3ID, Roles: []string{"org_participant"}}, nil); err != nil {
		t.Fatalf("Should have been able to make user3 a org_participant: %v", err)
	}
	if _, err := user3Cl.Collection("org").Item(org1.OrgID).Read(&anOrg); err != nil {
		t.Fatalf("User3 should have been able to read the org: got error %v", err)
	}
	if status, _ = user3Cl.Collection("org/role").WithParent(org1.OrgID).Create(
		Role{UserId: user3ID, Roles: []string{"org_participant"}}, nil); status != 401 {
		t.Fatalf("User3 should not have been able to edit roles: %v", err)
	}

	if status, _ := user1Cl.Collection("org").Item(org1.OrgID).Read(&anOrg); status != 200 {
		t.Fatalf("Should have been able to read the org after having permission granted: got status %d", status)
	}
	aTeam := Team{}
	if status, _ := user1Cl.Collection("org/team").WithParent(org1.OrgID).Create(aTeam, &aTeam); status != 201 {
		t.Fatalf("Should have been able to create the team: got status %d", status)
	}
	if status, _ := user2Cl.Collection("org/team").WithParent(org1.OrgID).Create(aTeam, nil); status != 401 {
		t.Fatalf("Should not have been able to create the team: got status %d", status)
	}

	teamItem := user2Cl.Collection("org/team").WithParent(org1.OrgID).Item(aTeam.TeamID)
	if status, _ := teamItem.Read(&aTeam); status != 401 {
		t.Fatalf("Should not have been able to read team: got status %d", status)
	}
	if status, _ := user1Cl.Collection("org/team").WithParent(org1.OrgID).
		Item(aTeam.TeamID).Subcollection("role").Create(
		Role{
			Roles:  []string{"team_admin"},
			UserId: user2ID,
		}, &aTeam); status != 201 {
		t.Fatalf("Should have been able to create the role: got status %d", status)
	}
	if status, _ := teamItem.Read(&aTeam); status != 200 {
		t.Fatalf("Should have been able to read team: got status %d", status)
	}

	// test authentication
	auth := []extensions.ResourceWithRoles{}
	_, err = user1Cl.RawGet("/authorization/org", &auth)
	if err != nil {
		t.Fatalf("Should have been able to get org's authorization: %v", err)
	}
	if len(auth) != 1 {
		t.Fatalf("Should have found one org: %v", err)
	}
	if auth[0].ID.String() != org1.OrgID.String() {
		t.Fatalf("Should have found the org1: %v", err)
	}
	if len(auth[0].Roles) != 1 {
		t.Fatalf("Should have found one role: %v", err)
	}
	if auth[0].Roles[0] != "org_admin" {
		t.Fatalf("Should have found the org1: %v", err)
	}
	auth = []extensions.ResourceWithRoles{}
	_, err = user1Cl.RawGet("/authorization/org/team", &auth)
	if err != nil {
		t.Fatalf("Should have been able to list team's authorization: %v", err)
	}
	if len(auth) != 0 {
		t.Fatalf("Should have found no team: %v", err)
	}
	auth = []extensions.ResourceWithRoles{}
	_, err = user2Cl.RawGet("/authorization/org/team", &auth)
	if err != nil {
		t.Fatalf("Should have been able to list my orgs: %v", err)
	}
	if len(auth) != 1 {
		t.Fatalf("Should have found one team: %v", err)
	}
	if auth[0].ID.String() != aTeam.TeamID.String() {
		t.Fatalf("Should have found the team1: got %v", auth[0].ID.String())
	}
}
