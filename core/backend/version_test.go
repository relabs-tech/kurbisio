// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

import (
	"testing"

	"github.com/relabs-tech/kurbisio/core/backend"
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

	backend.Version = "another version"

	_, err = testService.client.RawGet("/version", &version)
	if err != nil {
		t.Fatal(err)
	}
	if version.Version != "another version" {
		t.Fatalf("Execting 'another version', got %s", version)
	}
}
