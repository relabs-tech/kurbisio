package client

import (
	"os"
	"testing"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestCient(t *testing.T) {

	client := NewWithRouter(nil)

	parentID := uuid.MustParse("4f1638da-861e-4a81-8cc7-e6847b6fdf9b")
	childID := uuid.MustParse("c46da255-eb72-4cc6-8835-1b34a9917826")

	collection := client.Collection("parent/child")
	if p := collection.CollectionPath(); p != "/parents/all/children" {
		t.Fatal("unexpected collection path:", p)
	}

	item := collection.Item(childID)
	if p := item.Path(); p != "/parents/all/children/"+childID.String() {
		t.Fatal("unexpected item path:", p)
	}

	collection = client.Collection("parent/child").WithParent(parentID)
	if p := collection.CollectionPath(); p != "/parents/"+parentID.String()+"/children" {
		t.Fatal("unexpected collection path:", p)
	}

	item = collection.Singleton()
	if p := item.Path(); p != "/parents/"+parentID.String()+"/child" {
		t.Fatal("unexpected item path:", p)
	}

	collection = client.Collection("parent/child").WithFilter("email", "maybe@yes.no").WithParameter("something", "else")
	if p := collection.CollectionPath(); p != "/parents/all/children?filter=email=maybe@yes.no&something=else" {
		t.Fatal("unexpected collection path:", p)
	}

	// filter really is a only a shortcut for WithParameter
	collection = client.Collection("parent/child").WithParameter("filter", "email=maybe@yes.no").WithParameter("something", "else")
	if p := collection.CollectionPath(); p != "/parents/all/children?filter=email=maybe@yes.no&something=else" {
		t.Fatal("unexpected collection path:", p)
	}

}
