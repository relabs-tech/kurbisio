// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package access

import (
	"fmt"

	"github.com/goccy/go-json"

	"github.com/relabs-tech/kurbisio/core/csql"
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
