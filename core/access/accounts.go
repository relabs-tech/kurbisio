package access

import (
	"encoding/json"
	"fmt"

	"github.com/relabs-tech/backends/core/csql"
)

// FunctionAccount is a function accoutn
type FunctionAccount struct {
	Identity string
	Roles    []string
}

// EnsureFunctionAccounts creates the specified function accounts if they do not exist yet
func EnsureFunctionAccounts(db *csql.DB, accounts ...FunctionAccount) error {
	insertQuery := fmt.Sprintf("INSERT INTO %s.account (identity,properties) VALUES($1,$2) ON CONFLICT DO NOTHING;", db.Schema)
	type Roles struct {
		Roles []string `json:"roles"`
	}
	for _, account := range accounts {
		properties, _ := json.Marshal(Roles{Roles: account.Roles})
		_, err := db.Exec(insertQuery, account.Identity, properties)
		if err != nil {
			return err
		}
	}
	return nil
}
