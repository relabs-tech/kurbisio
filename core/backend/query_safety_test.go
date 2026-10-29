package backend_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/relabs-tech/kurbisio/core/client"
)

var querySafetyConfiguration = `{
	"collections": [
		{ "resource": "org" },
		{ "resource": "org/team" },
		{ "resource": "org/player", "searchable_properties": ["tag"] }
	],
	"relations": [
		{ "resource": "org/member", "left": "org/team", "right": "org/player" }
	]
}`

type safetyOrg struct {
	OrgID uuid.UUID `json:"org_id"`
}

type safetyTeam struct {
	TeamID uuid.UUID `json:"team_id"`
	OrgID  uuid.UUID `json:"org_id"`
}

type safetyPlayer struct {
	PlayerID uuid.UUID `json:"player_id"`
	OrgID    uuid.UUID `json:"org_id"`
	Tag      string    `json:"tag,omitempty"`
	Name     string    `json:"name,omitempty"`
	Revision int       `json:"revision,omitempty"`
}

type safetyMember struct {
	TeamID   uuid.UUID `json:"team_id"`
	PlayerID uuid.UUID `json:"player_id"`
	OrgID    uuid.UUID `json:"org_id"`
	Revision int       `json:"revision,omitempty"`
}

type safetyFixture struct {
	client  client.Client
	service *TestService
	orgs    [2]safetyOrg
	teams   [2]safetyTeam
	players [2]safetyPlayer
}

// newSafetyFixture creates two organizations, each with one team, one player
// and one membership.
func newSafetyFixture(t *testing.T) *safetyFixture {
	t.Helper()
	s := CreateTestService(querySafetyConfiguration, t.Name())
	t.Cleanup(s.Db.Close)
	f := &safetyFixture{client: s.client, service: s}
	for i := range f.orgs {
		if _, err := f.client.RawPost("/orgs", &safetyOrg{}, &f.orgs[i]); err != nil {
			t.Fatal(err)
		}
		org := f.org(i)
		if _, err := f.client.RawPost(org+"/teams", &safetyTeam{}, &f.teams[i]); err != nil {
			t.Fatal(err)
		}
		player := safetyPlayer{Tag: "tag", Name: []string{"one", "two"}[i]}
		if _, err := f.client.RawPost(org+"/players", &player, &f.players[i]); err != nil {
			t.Fatal(err)
		}
		member := safetyMember{TeamID: f.teams[i].TeamID, PlayerID: f.players[i].PlayerID}
		if _, err := f.client.RawPut(org+"/members", &member, nil); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *safetyFixture) org(i int) string {
	return "/orgs/" + f.orgs[i].OrgID.String()
}

// do sends a request through the router and returns the recorded response.
func (f *safetyFixture) do(method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r = r.WithContext(f.client.Context())
	rec := httptest.NewRecorder()
	f.service.Router.ServeHTTP(rec, r)
	return rec
}

// injectedFilter is a property filter whose key would turn the WHERE clause
// into a tautology across parents if it were interpolated into SQL.
var injectedFilter = "filter=" + url.QueryEscape(`x' IS NULL) OR (true) OR (properties->>'x=v`)

func TestPropertyFilterKeyIsBoundAsData(t *testing.T) {
	f := newSafetyFixture(t)

	for _, list := range []string{"/players", "/members"} {
		for _, paging := range []string{"", "&page=1"} {
			var result []map[string]any
			path := f.org(0) + list + "?" + injectedFilter + paging
			if _, err := f.client.RawGet(path, &result); err != nil {
				t.Fatal(path, err)
			}
			if len(result) != 0 {
				t.Fatalf("%s: injected key matched %d items", path, len(result))
			}
		}
	}

	ids := `["` + f.players[0].PlayerID.String() + `","` + f.players[1].PlayerID.String() + `"]`
	rec := f.do(http.MethodPost, f.org(0)+"/players/mget?"+injectedFilter, ids)
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("mget with injected key: %d %s", rec.Code, rec.Body.String())
	}

	if rec := f.do(http.MethodDelete, f.org(0)+"/players?"+injectedFilter, ""); rec.Code >= 300 {
		t.Fatalf("clear with injected key: %d %s", rec.Code, rec.Body.String())
	}
	for i := range f.players {
		path := f.org(i) + "/players/" + f.players[i].PlayerID.String()
		if _, err := f.client.RawGet(path, &safetyPlayer{}); err != nil {
			t.Fatalf("clear with injected key removed %s: %v", path, err)
		}
	}

	// Ordinary property filters keep working.
	var result []safetyPlayer
	if _, err := f.client.RawGet("/orgs/all/players?filter=name=two", &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0].PlayerID != f.players[1].PlayerID {
		t.Fatalf("property filter: %+v", result)
	}
}

func TestMultiGetCombinesColumnAndPropertyFilters(t *testing.T) {
	f := newSafetyFixture(t)

	ids := `["` + f.players[0].PlayerID.String() + `","` + f.players[1].PlayerID.String() + `"]`
	rec := f.do(http.MethodPost, "/orgs/all/players/mget?filter=tag=tag&filter=name=two", ids)
	if rec.Code != http.StatusOK {
		t.Fatalf("mget: %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), f.players[1].PlayerID.String()) ||
		strings.Contains(rec.Body.String(), f.players[0].PlayerID.String()) {
		t.Fatalf("mget returned the wrong items: %s", rec.Body.String())
	}
}

func TestRelationAscendingCursorVisitsEveryItemOnce(t *testing.T) {
	f := newSafetyFixture(t)
	org := f.org(0)
	player := f.players[0].PlayerID
	timestamp := "2026-01-02T03:04:05Z"

	want := map[uuid.UUID]bool{}
	for i := 0; i < 5; i++ {
		var team safetyTeam
		if _, err := f.client.RawPost(org+"/teams", &safetyTeam{}, &team); err != nil {
			t.Fatal(err)
		}
		body := map[string]any{"team_id": team.TeamID, "player_id": player, "timestamp": timestamp}
		if _, err := f.client.RawPut(org+"/members", body, nil); err != nil {
			t.Fatal(err)
		}
		want[team.TeamID] = true
	}

	seen := map[uuid.UUID]int{}
	path := org + "/members?order=asc&limit=1&from=" + timestamp + "&until=" + timestamp
	for pages := 0; path != ""; pages++ {
		if pages > 10 {
			t.Fatal("cursor did not terminate")
		}
		var page []safetyMember
		_, header, err := f.client.RawGetWithHeader(path, nil, &page)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range page {
			seen[m.TeamID]++
		}
		path = ""
		if next := header.Get("Pagination-Next-Token"); next != "" {
			path = org + "/members?order=asc&limit=1&next_token=" + url.QueryEscape(next)
		}
	}
	for team := range want {
		if seen[team] != 1 {
			t.Errorf("team %s seen %d times, want once: %v", team, seen[team], seen)
		}
	}
}

func TestRevisionConflictRequiresMatchingParents(t *testing.T) {
	f := newSafetyFixture(t)
	player := f.players[0].PlayerID.String()
	member := f.teams[0].TeamID.String() + ":" + player

	for _, path := range []string{
		f.org(1) + "/players/" + player,
		f.org(1) + "/members/" + member,
	} {
		for _, method := range []string{http.MethodPut, http.MethodPatch} {
			rec := f.do(method, path, `{"name":"changed","revision":999}`)
			if rec.Code != http.StatusNotFound {
				t.Errorf("%s %s under another parent: %d %s", method, path, rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), player) {
				t.Errorf("%s %s exposed the stored resource: %s", method, path, rec.Body.String())
			}
		}
	}

	// Within the owning parent the conflict still returns the current resource.
	for _, path := range []string{
		f.org(0) + "/players/" + player,
		f.org(0) + "/members/" + member,
	} {
		rec := f.do(http.MethodPut, path, `{"name":"changed","revision":999}`)
		if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), player) {
			t.Errorf("PUT %s conflict: %d %s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestMultiGetRequiresUUIDArray(t *testing.T) {
	f := newSafetyFixture(t)
	path := f.org(0) + "/players/mget"

	for _, body := range []string{`null`, `[null]`, `[1]`, `{}`, `["not-a-uuid"]`} {
		if rec := f.do(http.MethodPost, path, body); rec.Code != http.StatusBadRequest {
			t.Errorf("mget %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	for _, body := range []string{`["00000000-0000-0000-0000-000000000000"]`} {
		rec := f.do(http.MethodPost, path, body)
		if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "[]" {
			t.Errorf("mget %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
}
