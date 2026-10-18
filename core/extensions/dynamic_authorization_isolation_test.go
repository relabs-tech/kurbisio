package extensions_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/relabs-tech/kurbisio/core/extensions"
)

// TestDynamicAuthRequestIsolation exercises dynamic permissions against PostgreSQL
// through reused internal contexts and the real JWT middleware, including its cache.
func TestDynamicAuthRequestIsolation(t *testing.T) {
	service := createTestService(`{"collections":[
 {"resource":"account","external_index":"identity","searchable_properties":["email"]},
 {"resource":"club","permits":[
 {"role":"club_admin","operations":["read","update"],"selectors":["club"]},
 {"role":"club_viewer","operations":["read"],"selectors":["club"]}]},
 {"resource":"club/role"}]}`, t.Name(), extensions.DynamicAuth{
		TargetCollection: "club", RolesCollection: "club/role", Selector: "user", AllowedRoles: []string{"club_admin", "club_viewer"},
	})
	t.Cleanup(func() {
		if err := service.close(); err != nil {
			t.Error(err)
		}
	})
	admin := service.client.WithAdminAuthorization()
	userID := uuid.New()
	base := &access.Authorization{Roles: []string{"user"}, Selectors: map[string]string{"user_id": userID.String()}}
	expected := base.Clone()
	// One user administers A, can only read B, and has no access to C. This
	// distinguishes successful discovery from accidental permission escalation.
	clubs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for i, id := range clubs {
		if _, err := admin.Collection("club").Upsert(map[string]any{"club_id": id}, nil); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			role := []string{"club_admin", "club_viewer"}[i]
			if _, err := admin.Collection("club/role").WithParent(id).Create(map[string]any{"user_id": userID, "roles": []string{role}}, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Returning to A after visiting B detects a selector retained from either club.
	paths := []string{"/clubs/" + clubs[0].String(), "/clubs/" + clubs[1].String(), "/clubs/" + clubs[0].String()}
	t.Run("internal client and parent context", func(t *testing.T) {
		// Internal calls reuse the same context pointer without a JWT cache to
		// mask mutations; each routed request must derive its own authorization.
		ctx := access.ContextWithAuthorization(context.Background(), base)
		cl := client.NewWithRouter(service.backend.Router()).WithContext(ctx)
		for _, path := range paths {
			if status, err := cl.RawGet(path, nil); status != 200 || err != nil {
				t.Fatalf("%s: %d %v", path, status, err)
			}
		}
		if !reflect.DeepEqual(base, expected) {
			t.Fatalf("caller authorization mutated: %#v", base)
		}
	})
	t.Run("explicit conflicting selector", func(t *testing.T) {
		// Membership in B must not override an explicit scope restricting this
		// authorization to A, even when it already carries an administrator role.
		scoped := base.Clone()
		scoped.Selectors["club_id"] = clubs[0].String()
		scoped.Roles = append(scoped.Roles, "club_admin")
		cl := client.NewWithRouter(service.backend.Router()).WithAuthorization(scoped)
		if status, _ := cl.RawGet(paths[1], nil); status != http.StatusUnauthorized && status != http.StatusForbidden {
			t.Fatalf("conflicting scope granted: %d", status)
		}
	})
	// A nested router call must not change the authorization used by its parent.
	service.backend.Router().HandleFunc("/aggregate", func(w http.ResponseWriter, r *http.Request) {
		auth := access.AuthorizationFromContext(r.Context())
		before := auth.Clone()
		cl := client.NewWithRouter(service.backend.Router()).WithContext(r.Context())
		for _, path := range paths {
			if status, err := cl.RawGet(path, nil); status != 200 || err != nil {
				http.Error(w, fmt.Sprint(status, err), 500)
				return
			}
		}
		if !reflect.DeepEqual(auth, before) {
			http.Error(w, "parent authorization mutated", 500)
			return
		}
		w.WriteHeader(200)
	})
	// Account lookup uses the issuer/subject identity encoded in the signed JWT.
	const issuer = "https://issuer.example"
	subject := uuid.NewString()
	account := map[string]any{"identity": issuer + "|" + subject, "email": "", "roles": expected.Roles, "selectors": expected.Selectors}
	if _, err := admin.Collection("account").Create(account, nil); err != nil {
		t.Fatal(err)
	}
	// Serve a local signing key in the issuer endpoint format expected by the
	// middleware, so signature verification runs without an external identity service.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}))
	keys := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"local": publicPEM})
	}))
	t.Cleanup(keys.Close)
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{Issuer: issuer, Subject: subject, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))})
	token.Header["kid"] = "local"
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	// Keep one middleware instance for every HTTP request: rebuilding it would
	// reset the token cache and miss the original cross-request contamination.
	handler := access.NewJwtMiddelware2(&access.JwtMiddlewareBuilder{DB: service.db, Issuers: []access.IdentityIssuer{{Name: issuer, PublicKeyDownloadURL: keys.URL}}})(service.backend.Router())
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	httpClient := &http.Client{Timeout: 5 * time.Second}
	request := func(method, path string) (int, error) {
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(`{"name":"changed"}`))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Authorization", "Bearer "+signed)
		req.Header.Set("Content-Type", "application/json")
		res, err := httpClient.Do(req)
		if err != nil {
			return 0, err
		}
		defer res.Body.Close()
		_, err = io.Copy(io.Discard, res.Body)
		return res.StatusCode, err
	}
	// First request loads the account. Removing its lookup identity then proves
	// subsequent successful requests use the token cache rather than another lookup.
	if status, err := request("GET", paths[0]); status != 200 || err != nil {
		t.Fatalf("initial JWT account lookup: %d %v", status, err)
	}
	if _, err := service.db.Exec(context.Background(), "UPDATE "+service.db.Schema+".account SET identity=$1 WHERE identity=$2", issuer+"|removed", issuer+"|"+subject); err != nil {
		t.Fatal(err)
	}
	for _, path := range append(paths[1:], "/aggregate") {
		if status, err := request("GET", path); status != 200 || err != nil {
			t.Fatalf("JWT GET %s: %d %v", path, status, err)
		}
	}
	// A cached administrator grant for A must neither enable writes in B nor
	// permit access to C. A must still retain its legitimate write permission.
	for _, check := range []struct{ method, path string }{{"PUT", paths[1]}, {"GET", "/clubs/" + clubs[2].String()}} {
		if status, err := request(check.method, check.path); err != nil || (status != 401 && status != 403) {
			t.Fatalf("permission isolation %s: %d %v", check.path, status, err)
		}
	}
	if status, err := request("PUT", paths[0]); status != 200 || err != nil {
		t.Fatalf("admin write: %d %v", status, err)
	}
	// Concurrent cache hits for A and B must also remain independent. Run this
	// regression with -race to detect shared slice, map, or middleware state.
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := paths[i%2]
			if status, err := request("GET", path); status != 200 || err != nil {
				t.Errorf("concurrent GET %s: %d %v", path, status, err)
			}
		}()
	}
	wg.Wait()
}
