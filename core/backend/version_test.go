package backend

import (
	"testing"
)

// TestVersion verifies that the /version endpoint works
func TestVersion(t *testing.T) {
	var version struct {
		Version string `json:"version"`
	}
	_, err := testService.client.RawGet("/version", &version)
	if err != nil {
		t.Fatal(err)
	}
	if version.Version != "unset" {
		t.Fatalf("Expecting 'unset' version by default, got %s", version)
	}

	Version = "another version"

	_, err = testService.client.RawGet("/version", &version)
	if err != nil {
		t.Fatal(err)
	}
	if version.Version != "another version" {
		t.Fatalf("Execting 'another version', got %s", version)
	}
}
