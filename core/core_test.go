package core

import (
	"encoding/json"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestOperations_JSON_Unmarshalling(t *testing.T) {

	type Object struct {
		Operations []Operation `json:"operations"`
	}
	var object Object
	jsonRead := `{"operations":["create","read","update","list"]}`
	err := json.Unmarshal([]byte(jsonRead), &object)
	if err != nil {
		t.Fatal(err)
	}

	jsonRead = `{"operations":["invalid"]}`
	err = json.Unmarshal([]byte(jsonRead), &object)
	if err == nil {
		t.Fatal("invalid operation accepted")
	}

}
