package extensions_test

import (
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/relabs-tech/kurbisio/core/csql"
)

type TestService struct {
	Postgres         string `env:"POSTGRES,required" description:"the connection string for the Postgres DB without password"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,optional" description:"password to the Postgres DB"`
	backend          *backend.Backend
	client           client.Client
	close            func() error
}

// create a user and return the id of the account and a client with the authorization
// for the user
func (t TestService) createAccount() (uuid.UUID, *client.Client, error) {
	id := uuid.New()
	idUser := uuid.New()
	selectors := map[string]string{
		"account_id": id.String(),
		"user_id":    idUser.String(),
	}
	account := map[string]any{
		"account_id": id.String(),
		"roles":      []string{"user"},
		"selectors":  selectors,
	}
	user := map[string]any{
		"user_id":   idUser.String(),
		"something": "else",
	}
	_, err := t.client.WithAdminAuthorization().Collection("account").Upsert(account, &account)
	if err != nil {
		return idUser, nil, err
	}
	_, err = t.client.WithAdminAuthorization().Collection("user").Upsert(user, &user)
	if err != nil {
		return idUser, nil, err
	}
	cl := client.NewWithRouter(t.backend.Router()).WithAuthorization(&access.Authorization{
		Roles:     []string{""},
		Selectors: selectors,
	})
	return idUser, &cl, nil

}

func createTestService(config, schemaName string, extensions ...backend.KExtension) *TestService {

	s := TestService{}
	if err := envdecode.Decode(&s); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(s.Postgres, s.PostgresPassword, schemaName)
	db.ClearSchema()

	builder := backend.Builder{
		Config:               config,
		DB:                   db,
		Router:               mux.NewRouter(),
		AuthorizationEnabled: true,
		UpdateSchema:         true,
		Extensions:           extensions,
	}
	s.close = db.Close
	s.backend = backend.New(&builder)
	s.client = client.NewWithRouter(builder.Router)

	return &s
}
