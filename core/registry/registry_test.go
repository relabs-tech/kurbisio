// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package registry

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/kurbisio/core/csql"

	_ "github.com/lib/pq"
)

// TestService holds the configuration for this service
//
// use POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
// and POSTRGRES_PASSWORD="docker"
type TestService struct {
	Postgres         string `env:"POSTGRES,required" description:"the connection string for the Postgres DB without password"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,optional" description:"password to the Postgres DB"`
	registry         Registry
}

var testService TestService

func TestMain(m *testing.M) {
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_core_registry_unit_test_")
	defer db.Close()
	db.ClearSchema()

	testService.registry = New(db)

	code := m.Run()
	os.Exit(code)
}

func TestRegistry(t *testing.T) {

	type foo struct {
		A string
		B string
	}

	write := foo{
		A: "Hello",
		B: "World",
	}

	testRegistry := testService.registry.Accessor("_test_")

	// test non-existing key
	var something interface{}
	timestamp, err := testRegistry.Read("key does not exist", something)
	if err != nil {
		t.Fatal(err)
	}
	if !timestamp.IsZero() {
		t.Fatal("non existing key seems to exist")
	}

	now := time.Now()
	err = testRegistry.Write("test", write)
	if err != nil {
		t.Fatal(err)
	}
	var read foo
	timestamp, err = testRegistry.Read("test", &read)
	if err != nil {
		t.Fatal(err)
	}

	if read.A != write.A || read.B != write.B {
		t.Fatal("could not read what I wrote")
	}
	if timestamp.Sub(now) > time.Second {
		t.Fatal("created at is off")
	}

	// test that we can delete
	testRegistry.Delete("test")
	if err != nil {
		t.Fatal(err)
	}
	timestamp, err = testRegistry.Read("test", something)
	if err != nil {
		t.Fatal(err)
	}
	if !timestamp.IsZero() {
		t.Fatal("Deleted key still exists")
	}

}

// BenchmarkLoggedVsUnlogged compares the performance of logged and unlogged registries
func BenchmarkLoggedVsUnlogged(b *testing.B) {
	type testData struct {
		ID          int
		Name        string
		Description string
		Timestamp   time.Time
		Values      []float64
	}

	// Setup databases
	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_core_registry_benchmark_test_")
	defer db.Close()
	db.ClearSchema()

	loggedRegistry := New(db)
	unloggedRegistry := NewUnlogged(db)

	loggedAccessor := loggedRegistry.Accessor("benchmark")
	unloggedAccessor := unloggedRegistry.Accessor("benchmark")

	testValue := testData{
		ID:          42,
		Name:        "Test Object",
		Description: "This is a test object for benchmarking registry performance",
		Timestamp:   time.Now(),
		Values:      []float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9},
	}

	// Write to different keys to force actual disk I/O
	b.Run("Logged-Write-UniqueKeys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("test_key_%d", i) // unique key each iteration
			err := loggedAccessor.Write(key, testValue)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Unlogged-Write-UniqueKeys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("test_key_%d", i) // unique key each iteration
			err := unloggedAccessor.Write(key, testValue)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Pre-populate for read tests
	loggedAccessor.Write("read_test", testValue)
	unloggedAccessor.Write("read_test", testValue)

	b.Run("Logged-Read", func(b *testing.B) {
		var read testData
		for i := 0; i < b.N; i++ {
			_, err := loggedAccessor.Read("read_test", &read)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Unlogged-Read", func(b *testing.B) {
		var read testData
		for i := 0; i < b.N; i++ {
			_, err := unloggedAccessor.Read("read_test", &read)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Mixed read/write workload with unique keys
	b.Run("Logged-Mixed-UniqueKeys", func(b *testing.B) {
		var read testData
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				key := fmt.Sprintf("mixed_test_%d", i)
				err := loggedAccessor.Write(key, testValue)
				if err != nil {
					b.Fatal(err)
				}
			} else {
				_, err := loggedAccessor.Read("read_test", &read)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("Unlogged-Mixed-UniqueKeys", func(b *testing.B) {
		var read testData
		for i := 0; i < b.N; i++ {
			if i%2 == 0 {
				key := fmt.Sprintf("mixed_test_%d", i)
				err := unloggedAccessor.Write(key, testValue)
				if err != nil {
					b.Fatal(err)
				}
			} else {
				_, err := unloggedAccessor.Read("read_test", &read)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
