package extensions_test

import (
	"testing"

	"github.com/relabs-tech/kurbisio/core/extensions"
)

// TestAuthTokenInvalid verifies that the extension panics when the account collection is not present
func TestAuthTokenNoAccountCollection(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Should have panicked because there is no account collection")
		}
	}()

	testService := createTestService(`{}`, t.Name(), extensions.AuthToken{})
	defer testService.close()
}

// TestAuthTokenInvalid verifies that the extension panics when the account collection is not present
func TestAuthTokenNoShortCut(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Should have panicked because there is no account shortcut")
		}
	}()

	testService := createTestService(`{"collections": [{"resource": "account"}]}`,
		t.Name(), extensions.AuthToken{})
	defer testService.close()
}

func TestAuthToken(t *testing.T) {
	testService := createTestService(`
	{
		"collections": [
			{"resource": "account"},
			{"resource": "user",
			 "permits": [
				{
					"role": "user",
					"operations": [
						"read",
						"update"
					],
					"selectors": [
						"user"
					]
				}
			]
			},
			{"resource": "user/data",
						 "permits": [
				{
					"role": "user",
					"operations": [
						"read",
						"update",
						"create"
					],
					"selectors": [
						"user"
					]
				}
			]
		}
		],
		"shortcuts": [
			{"shortcut": "account", "target": "account", "roles": ["everybody"]},
			{"shortcut": "user", "target": "user", "roles": ["user"]}
		]
	}
	`, t.Name(), extensions.AuthToken{})
	defer testService.close()

	adminCl := testService.client.WithAdminAuthorization()
	tokens := []any{}
	_, err := adminCl.Collection("account/token_metadata").List(&tokens)
	if err != nil {
		t.Fatalf("Should have been able to list tokens: %v", err)
	}

	_, err = adminCl.Collection("account/token_metadata/token").List(&tokens)
	if err != nil {
		t.Fatalf("Should have been able to list tokens: %v", err)
	}

	_,userCl, err := testService.createAccount()
	if err != nil {
		t.Fatalf("Should have been able to create account: %v", err)
	}

	tok := extensions.TokenMetadataModel{
		Description: "test",
	}
	res := map[string]string{}
	_, err = userCl.RawPost("/_account/create_token", &tok, &res)
	if err != nil {
		t.Fatalf("Should have been able to create token: %v", err)
	}
	if res["token"] == "" {
		t.Fatalf("Should have been able to create token: %v", err)
	}

	_, err = userCl.RawGet("/account/token_metadatas", &tokens)
	if err != nil {
		t.Fatalf("Should have been able to list tokens: %v", err)
	}
	if len(tokens) != 1 {
		t.Fatalf("Should have been able to list tokens: %v", err)
	}
	if tokens[0].(map[string]any)["token"] != nil {
		t.Fatalf("Should not have been able to list tokens: %v", err)
	}

	status, _ := userCl.RawGet("/account/token_metadatas/all/token", &tokens)
	if status != 401 {
		t.Fatalf("Should not have been able to list tokens: %d", status)
	}

	user := map[string]any{}
	status, _, err = testService.client.RawGetWithHeader("/user",
		map[string]string{"Authorization": "AuthToken: " + res["token"]}, &user)
	if err != nil {
		t.Fatalf("Should have been able to read user: %v, status: %d", err, status)
	}

	data := map[string]any{}
	status, err = testService.client.RawPostWithHeader("/user/datas",
		map[string]string{"Authorization": "AuthToken: " + res["token"]}, &data, &data)
	if err != nil {
		t.Fatalf("Should have been able to create user/data: %v, status: %d", err, status)
	}

	status, _ = testService.client.RawPostWithHeader("/user/datas",
		map[string]string{"Authorization": "AuthToken: " + res["token"] + "invalid"}, &data, &data)
	if status != 401 {
		t.Fatalf("Should have not been able to create user/data: status: %d", status)
	}

}
