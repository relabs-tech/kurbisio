package access

import (
	"reflect"
	"testing"
)

// TestAuthorizationClone verifies that a copy preserves optional fields without
// sharing the mutable roles slice or selectors map with its source.
func TestAuthorizationClone(t *testing.T) {
	if (*Authorization)(nil).Clone() != nil {
		t.Fatal("nil receiver must remain nil")
	}
	// Distinguish absent fields from allocated but empty fields.
	for _, original := range []*Authorization{
		{},
		{Roles: []string{}, Selectors: map[string]string{}},
		{Roles: []string{"user"}, Selectors: map[string]string{"user_id": "one"}},
	} {
		copy := original.Clone()
		if copy == original || !reflect.DeepEqual(copy, original) {
			t.Fatalf("invalid clone: %#v", copy)
		}
		// Mutating existing entries catches shallow copies of either container.
		if len(copy.Roles) > 0 {
			copy.Roles[0] = "admin"
			copy.Selectors["user_id"] = "two"
			if original.Roles[0] != "user" || original.Selectors["user_id"] != "one" {
				t.Fatal("clone aliases original")
			}
		}
	}
}

// TestAuthorizationCacheCopies verifies that neither the writer nor a reader
// can change the authorization snapshot held for a token.
func TestAuthorizationCacheCopies(t *testing.T) {
	cache := NewAuthorizationCache()
	original := &Authorization{Roles: []string{"user"}, Selectors: map[string]string{"user_id": "one"}}
	expected := original.Clone()
	// Write must detach the cached snapshot before the caller changes its input.
	cache.Write("token", original)
	original.Roles[0] = "admin"
	original.Selectors["user_id"] = "two"
	// The second read must be unaffected by mutations to the first read result.
	for range 2 {
		got := cache.Read("token")
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("cache changed: %#v", got)
		}
		got.Roles[0] = "admin"
		got.Selectors["user_id"] = "three"
	}
	// Copying must preserve missing authorizations and absent optional fields.
	cache.Write("nil", nil)
	if cache.Read("missing") != nil || cache.Read("nil") != nil {
		t.Fatal("missing/nil entries must return nil")
	}
	cache.Write("empty", &Authorization{})
	if got := cache.Read("empty"); got.Roles != nil || got.Selectors != nil {
		t.Fatal("nil fields changed")
	}
}
