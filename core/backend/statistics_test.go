// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

import (
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/relabs-tech/kurbisio/core/backend"
)

// TestStatistics verifies that the /kurbisio/statistics endpoint returns information about the backend
func TestStatistics(t *testing.T) {

	testService := CreateTestService(configurationJSON, t.Name())
	defer testService.Db.Close()

	// Create resources to be sure that we have some valid statistics
	numberOfElements := 14
	for i := 1; i <= numberOfElements; i++ {
		_, err := testService.client.WithAdminAuthorization().RawPost("/as", A{ExternalID: t.Name() + strconv.Itoa(i)}, &A{})
		if err != nil {
			t.Fatal(err)
		}

		_, err = testService.client.WithAdminAuthorization().RawPostBlob("/blobs", map[string]string{}, []byte{0, 1, 2}, &Blob{})
		if err != nil {
			t.Fatal(err)
		}
	}

	var stats backend.StatisticsDetails
	_, h, err := testService.client.WithAdminAuthorization().RawGetWithHeader("/kurbisio/statistics", map[string]string{}, &stats)
	if err != nil {
		t.Fatal(err)
	}
	if h.Get("ETag") == "" {
		t.Fatal("ETag is empty")
	}

	var expectedResources, receivedResources sort.StringSlice

	// Get the list of expected resources statistics
	for _, r := range testService.backend.Config().Collections {
		expectedResources = append(expectedResources, r.Resource)
	}
	for _, r := range testService.backend.Config().Blobs {
		expectedResources = append(expectedResources, r.Resource)
	}
	for _, r := range testService.backend.Config().Relations {
		resource := ""
		if r.Resource != "" {
			resource = r.Resource + ":" + resource
		}
		resource += r.Left + ":" + r.Right
		expectedResources = append(expectedResources, resource)
	}
	for _, r := range testService.backend.Config().Singletons {
		expectedResources = append(expectedResources, r.Resource)
	}

	for _, r := range stats.Collections {
		receivedResources = append(receivedResources, r.Resource)
	}
	for _, r := range stats.Singletons {
		receivedResources = append(receivedResources, r.Resource)
	}
	for _, r := range stats.Relations {
		receivedResources = append(receivedResources, r.Resource)
	}
	for _, r := range stats.Blobs {
		receivedResources = append(receivedResources, r.Resource)
	}

	// Sort so that comparison between expected vs received is simple
	expectedResources.Sort()
	receivedResources.Sort()

	if !reflect.DeepEqual(expectedResources, receivedResources) {
		t.Fatalf("Expected %v resources statistics, got %v", expectedResources, receivedResources)
	}

	// Verify that we got non-null statistics for the resources we created at the beginning
	for _, r := range []string{"a", "blob"} {
		s := getResourceByName(r, stats)
		if s == nil {
			t.Fatalf("No statistics found about resource: %s", r)
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
	_, err = testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}
}

// TestStatisticsFiltered verifies that the /kurbisio/statistics endpoint returns information about the backend
func TestStatisticsFiltered(t *testing.T) {

	testService := CreateTestService(configurationJSON, t.Name())
	defer testService.Db.Close()

	var stats backend.StatisticsDetails
	_, h, err := testService.client.WithAdminAuthorization().RawGetWithHeader("/kurbisio/statistics?resource=a,b", map[string]string{}, &stats)
	if err != nil {
		t.Fatal(err)
	}
	if h.Get("ETag") == "" {
		t.Fatal("ETag is empty")
	}

	var receivedResources sort.StringSlice

	expectedResources := sort.StringSlice{"a", "b"}

	for _, r := range stats.Collections {
		receivedResources = append(receivedResources, r.Resource)
	}
	for _, r := range stats.Singletons {
		receivedResources = append(receivedResources, r.Resource)
	}
	for _, r := range stats.Relations {
		receivedResources = append(receivedResources, r.Resource)
	}
	for _, r := range stats.Blobs {
		receivedResources = append(receivedResources, r.Resource)
	}

	// Sort so that comparison between expected vs received is simple
	receivedResources.Sort()

	if !reflect.DeepEqual(expectedResources, receivedResources) {
		t.Fatalf("Expected %v resources statistics, got %v", expectedResources, receivedResources)
	}

}

func getResourceByName(name string, stats backend.StatisticsDetails) *backend.ResourceStatistics {
	for _, r := range stats.Collections {
		if r.Resource == name {
			return &r
		}
	}
	for _, r := range stats.Singletons {
		if r.Resource == name {
			return &r
		}
	}
	for _, r := range stats.Relations {
		if r.Resource == name {
			return &r
		}
	}
	for _, r := range stats.Blobs {
		if r.Resource == name {
			return &r
		}
	}
	return nil
}
