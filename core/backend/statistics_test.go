package backend

import (
	"reflect"
	"sort"
	"strconv"
	"testing"
)

// TestStatistics verifies that the /kurbisio/statistics endpoint returns information about the backend
func TestStatistics(t *testing.T) {
	// Create resources to be sure that we have some valid statistics
	numberOfElements := 14
	for i := 1; i <= numberOfElements; i++ {
		_, err := testService.client.RawPost("/as", A{ExternalID: t.Name() + strconv.Itoa(i)}, &A{})
		if err != nil {
			t.Fatal(err)
		}

		_, err = testService.client.RawPostBlob("/blobs", map[string]string{}, []byte{0, 1, 2}, &Blob{})
		if err != nil {
			t.Fatal(err)
		}
	}

	var stats statisticsDetails
	_, h, err := testService.client.RawGetWithHeader("/kurbisio/statistics", map[string]string{}, &stats)
	if err != nil {
		t.Fatal(err)
	}
	if h.Get("ETag") == "" {
		t.Fatal("ETag is empty")
	}

	var expectedResources, receivedResources sort.StringSlice

	// Get the list of expected resources statistics
	for _, r := range testService.backend.config.Collections {
		expectedResources = append(expectedResources, r.Resource)
	}
	for _, r := range testService.backend.config.Blobs {
		expectedResources = append(expectedResources, r.Resource)
	}
	for _, r := range testService.backend.config.Singletons {
		expectedResources = append(expectedResources, r.Resource)
	}

	for _, r := range stats.Resources {
		receivedResources = append(receivedResources, r.Resource)
	}

	// Sort so that comparison between expected vs received is simple
	expectedResources.Sort()
	receivedResources.Sort()

	if !reflect.DeepEqual(expectedResources, receivedResources) {
		t.Fatalf("Expected %v resources statistics, got %v", expectedResources, receivedResources)
	}

	// Verify that we got non-null statistics for the resources we created at the begining
	for _, r := range []string{"a", "blob"} {
		s := getResourceByName(r, stats)
		if s == nil {
			t.Fatal("No statistics found about resource: ", *s)
		}
		if s.Count < int64(numberOfElements) {
			t.Fatalf("Count is expected larger than %d for resource %v", numberOfElements, *s)
		}
		if s.SizeMB <= 0 {
			t.Fatalf("SizeMB is expected larger than 0 for resource %v", *s)
		}
		if s.AverageSizeB <= 0 {
			t.Fatalf("AverageSizeB is expected larger than 0 for resource %v", *s)
		}
	}

}

func getResourceByName(name string, stats statisticsDetails) *resourceStatistics {
	for _, r := range stats.Resources {
		if r.Resource == name {
			return &r
		}
	}
	return nil
}
