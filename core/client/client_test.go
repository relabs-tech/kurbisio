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

	client := New(nil)

	parentID := uuid.MustParse("4f1638da-861e-4a81-8cc7-e6847b6fdf9b")
	childID := uuid.MustParse("c46da255-eb72-4cc6-8835-1b34a9917826")

	request := client.Collection("parent/child").WithPrimary(childID)
	if p := request.CollectionPath(); p != "/parents/all/children" {
		t.Fatal("unexpected collection path:", p)
	}
	if p := request.ItemPath(); p != "/parents/all/children/"+childID.String() {
		t.Fatal("unexpected item path:", p)
	}

	request = client.Collection("parent/child").WithParent(parentID)
	if p := request.CollectionPath(); p != "/parents/"+parentID.String()+"/children" {
		t.Fatal("unexpected collection path:", p)
	}
	if p := request.ItemPath(); p != "/parents/"+parentID.String()+"/children/all" {
		t.Fatal("unexpected item path:", p)
	}

	request = client.Collection("parent/child").WithPrimary(childID).WithFilter("email", "maybe@yes.no").WithFilter("state", "new")
	if p := request.CollectionPath(); p != "/parents/all/children?email=maybe@yes.no&state=new" {
		t.Fatal("unexpected collection path:", p)
	}
	if p := request.ItemPath(); p != "/parents/all/children/"+childID.String() {
		t.Fatal("unexpected item path:", p)
	}

}
