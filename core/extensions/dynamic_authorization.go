package extensions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/relabs-tech/kurbisio/core/logger"
)

// The dynamic authorization extension adds role-based access control based on collections.
// It allows users to have different roles for different resources in a collection.
//
// How it works:
// 1. Configuration parameters:
//    - `target_collection`: Collection to add dynamic authorization for
//    - `selector`: Field used to identify users in the roles collection
//    - `roles`: Allowed roles that can be added by this extension
//    - `roles_collection`: Collection containing role assignments
//
// 2. Requirements:
//    - A collection must exist at `<target_collection>/<roles_collection>`
//    - This collection needs:
//      * Searchable field `<selector>_id` to match user selectors
//      * Field `roles` containing an array of allowed role strings
//      * Matching selector in the account collection
//
// The extension adds middleware that:
//   - Checks if request path matches target_collection
//   - Reads roles collection to find entries matching the user's selector
//   - Adds matching roles to the request context
//   - Adds target_collection's ID as a selector
//
// It also adds GET /authorization/<target_collection>/ endpoint that returns user roles.
//
// Example config:
// - target_collection: "organization/team"
// - selector: "user"
// - roles: ["team_admin", "team_participant"]
// - roles_collection: "team_roles"
//
// Given collections:
// - organization
// - organization/team with permissions for team_admin and team_participant roles
// - organization/team/team_roles
// - account with user selector
//
// For resources:
// - /organizations/1/team/2
// - /organizations/1/team/2/team_roles/3 with {user_id: 123, roles: ["team_admin"]}
//
// User 123 accessing /organizations/1/team/2 gets team_admin role + team_id=2 selector
// Users without matching entries get no additional roles or access.

type DynamicAuth struct {
	AllowedRoles     []string
	TargetCollection string
	Selector         string
	RolesCollection  string
}

// GetName returns the name of the extension
func (e DynamicAuth) GetName() string {
	return "Dynamic for role " + e.TargetCollection
}

// UpdateConfig update checks if the target collection is present in the configuration
func (e DynamicAuth) UpdateConfig(config backend.Configuration) (backend.Configuration, error) {
	found := false
	for _, s := range config.Collections {
		if s.Resource == e.TargetCollection {
			found = true
			break
		}
	}
	if !found {
		return config, fmt.Errorf("the collection %s is mandatory to be able to use the %s extension", e.TargetCollection, e.GetName())
	}

	found = false
	for _, s := range config.Collections {
		if s.Resource == e.RolesCollection {
			found = true
			break
		}
	}
	if !found {
		return config, fmt.Errorf("the roles collection %s is mandatory to be able to use the %s extension", e.TargetCollection, e.GetName())
	}

	if !strings.HasPrefix(e.RolesCollection, e.TargetCollection) {
		return config, fmt.Errorf("the roles collection %s must be a sub collection of the target collection %s", e.RolesCollection, e.TargetCollection)
	}

	if e.RolesCollection == "" {
		return config, fmt.Errorf("the %s is mandatory to be able to use the %s extension", e.RolesCollection, e.GetName())
	}
	if e.Selector == "" {
		return config, fmt.Errorf("the %s is mandatory to be able to use the %s extension", e.Selector, e.GetName())
	}
	if len(e.AllowedRoles) == 0 {
		return config, fmt.Errorf("the %s is mandatory to be able to use the %s extension", e.AllowedRoles, e.GetName())
	}

	return config, nil
}

// UpdateMux updates the mux router with the extension routes
func (e DynamicAuth) UpdateMux(router *mux.Router) error {
	resources := strings.Split(e.TargetCollection, "/")
	router.Use(func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodOptions {
				h.ServeHTTP(w, r)
				return
			}
			path := r.URL.Path
			// Check if the path template matches the target collection or a subpath

			path = strings.TrimPrefix(path, "/")
			targetSelector := ""
			lastResource := resources[len(resources)-1]

			pathElements := strings.Split(path, "/")
			// true if the path is a subpath of the target collection
			hasTargetCollectionAsPrefix := false

			if len(pathElements) >= len(resources)*2 {
				hasTargetCollectionAsPrefix = true
				for i, p := range resources {
					if core.Plural(p) != pathElements[i*2] && p != pathElements[i*2] {
						hasTargetCollectionAsPrefix = false
						break
					}
				}
				targetSelector = pathElements[len(resources)*2-1]
				if targetSelector == "" || targetSelector == "all" {
					hasTargetCollectionAsPrefix = false
				}
			}
			if hasTargetCollectionAsPrefix {
				e.addAuth(router, lastResource, targetSelector, r)
			}
			h.ServeHTTP(w, r)
		})
	})

	// Add a new endpoint to get the roles for the user
	router.HandleFunc("/authorization/"+e.TargetCollection, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.Header()
			return
		}
		auth := access.AuthorizationFromContext(r.Context())
		if auth == nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var res []ResourceWithRoles
		var err error
		res, err = e.GetResourcesWithRoles(r.Context(), router, auth.Selectors)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	return nil
}

type ResourceWithRoles struct {
	Resource string    `json:"resource"`
	ID       uuid.UUID `json:"id"`
	Roles    []string  `json:"roles"`
}

// Returns the resources where the user has roles
func (e DynamicAuth) GetResourcesWithRoles(ctx context.Context, router *mux.Router, selectors map[string]string) ([]ResourceWithRoles, error) {
	adminClient := client.NewWithRouter(router).WithAdminAuthorization()
	rlog := logger.FromContext(ctx)

	selector, ok := selectors[e.Selector+"_id"]
	if !ok || selector == "" {
		return nil, nil
	}
	resources := strings.Split(e.TargetCollection, "/")
	lastResource := resources[len(resources)-1]
	result := map[uuid.UUID]ResourceWithRoles{}
	for p := adminClient.
		Collection(e.RolesCollection).WithFilter(e.Selector+"_id", selector).FirstPage(); p.HasData(); p = p.Next() {
		var roles []map[string]any
		_, err := p.Get(&roles)
		if err != nil {
			return nil, fmt.Errorf("cannot get roles for %s: %w", selector, err)
		}

		for _, role := range roles {
			id, ok := role[lastResource+"_id"].(string)
			if !ok {
				continue
			}
			var ID uuid.UUID
			ID, err = uuid.Parse(id)
			if err != nil {
				rlog.Errorf("cannot parse %s: %v", id, err)
				continue
			}
			if rolesData, ok := role["roles"].([]any); ok {
				roles := []string{}
				for _, role := range rolesData {
					roleString, ok := role.(string)
					if ok {
						roles = append(roles, roleString)
					}
				}
				if len(roles) == 0 {
					continue
				}
				res, ok := result[ID]
				if !ok {
					result[ID] = ResourceWithRoles{
						Roles:    roles,
						Resource: e.TargetCollection,
						ID:       ID,
					}
				} else {
					res.Roles = append(res.Roles, roles...)
				}
			}
		}
	}

	var resultList []ResourceWithRoles
	for _, r := range result {
		resultList = append(resultList, r)
	}
	return resultList, nil

}

type role struct {
	Roles []string `json:"roles"`
}

func (e DynamicAuth) addAuth(router *mux.Router, lastResource, targetSelector string, r *http.Request) {
	rlog := logger.FromContext(r.Context())
	auth := access.AuthorizationFromContext(r.Context())
	if auth == nil {
		rlog.Error("No authorization found")
		return
	}
	selector, ok := auth.Selectors[e.Selector+"_id"]
	if !ok {
		return
	}
	parentID, err := uuid.Parse(targetSelector)
	if err != nil {
		rlog.WithError(err).Errorf("cannot parse %s", targetSelector)
		return
	}

	if v, ok := auth.Selectors[lastResource+"_id"]; ok && v != targetSelector {
		rlog.Errorf("Overriding existing authorization for %s, selector: %s, was set to %s", lastResource, targetSelector, v)
		return
	}

	adminClient := client.NewWithRouter(router).WithAdminAuthorization()
	addedRole := false
	for p := adminClient.
		Collection(e.RolesCollection).
		WithParent(parentID).WithFilter(e.Selector+"_id", selector).FirstPage(); p.HasData(); p = p.Next() {
		var roles []role
		_, err := p.Get(&roles)
		if err != nil {
			rlog.WithError(err).Errorf("cannot get roles for %s", selector)
			return
		}

		for _, r := range roles {
			for _, role := range r.Roles {
				if role == "" {
					continue
				}
				if !slices.Contains(e.AllowedRoles, role) {
					rlog.Errorf("role %s is not allowed for %s", role, selector)
					continue
				}
				if !slices.Contains(auth.Roles, role) {
					auth.Roles = append(auth.Roles, role)
				}
				addedRole = true
			}
		}
	}
	if !addedRole {
		return
	}
	rlog.Infof("Adding authorization for %s, selector: %s", lastResource, targetSelector)
	auth.Selectors[lastResource+"_id"] = targetSelector
}
