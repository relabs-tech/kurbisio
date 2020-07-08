package schema_test

import (
	"github.com/relabs-tech/backends/core/schema"
	"testing"
)

const (
	ref1 = `{ "type" : "string" ,
		      "$id" : "http://some_host.com/string.json"}`
	ref2 = `{ "$id" : "http://some_host.com/maxlength.json",
	 		  "maxLength" : 5 }`

	top_level1 = `
	{ "$id" : "http://some_host.com/top1.json",
	  "allOf" : [
		{ "$ref" : "http://some_host.com/string.json" },
		{ "$ref" : "http://some_host.com/maxlength.json" }
		]
	}`
	top_level2 = `
	{ "$id" : "http://some_host.com/top2.json",
	  "allOf" : [
 		{ "$ref" : "http://some_host.com/string.json" },
 		{ "type": "string", "minlength": 3 }
	  ]
	}`
)

func TestValidateString(t *testing.T) {
	v, err := schema.NewValidator([]string{top_level1, top_level2}, []string{ref1, ref2})
	if err != nil {
		t.Fatalf("No error expected when creating validator, got %v", err)
	}

	schemaID1 := "http://some_host.com/top1.json"
	schemaID2 := "http://some_host.com/top2.json"
	jsonShortString := `"short"`
	jsonLongString := `"a very long string"`

	// Valid json
	if err := v.ValidateString(jsonShortString, schemaID1); err != nil {
		t.Fatalf("%s is expected to be valid with schema %s. Reported error was: %v", jsonShortString, schemaID1, err)
	}

	// Invalid json
	if err := v.ValidateString(jsonLongString, schemaID1); err == nil {
		t.Fatalf("%s is expected to be invalid with schema %s. Reported error was: %v", jsonLongString, schemaID1, err)
	}

	// Valid json
	if err := v.ValidateString(jsonLongString, schemaID2); err != nil {
		t.Fatalf("%s is expected to be valid with schema %s. Reported error was: %v", jsonLongString, schemaID2, err)
	}
	// Valid json
	if err := v.ValidateString(jsonLongString, schemaID2); err != nil {
		t.Fatalf("%s is expected to be valid with schema %s. Reported error was: %v", jsonLongString, schemaID2, err)
	}

}

func TestValidateSruct(t *testing.T) {
	schema1 := `{
		"$id": "https://loyalty2you.com/schemas/workout-plan.json",
		"type": "object",
		"required": [
			"workouts"
		],
		"properties": {
			"workouts": {
				"type": "string"
			}
		}
	}`
	type WorkoutPlan struct {
		Workouts string `json:"workouts"`
	}

	v, err := schema.NewValidator([]string{schema1}, []string{})
	if err != nil {
		t.Fatalf("No error expected when creating validator, got %v", err)
	}

	// Valid json
	if err := v.ValidateStruct(WorkoutPlan{"something"}, "https://loyalty2you.com/schemas/workout-plan.json"); err != nil {
		t.Fatal()
	}

	// Invalid json
	type WorkoutPlanIncorrect struct {
		Workouts string `json:"workouts_wrong"`
	}
	if err := v.ValidateStruct(WorkoutPlanIncorrect{"something"}, "https://loyalty2you.com/schemas/workout-plan.json"); err == nil {
		t.Fatal()
	}
}
func TestHasSchema(t *testing.T) {
	v, err := schema.NewValidator([]string{top_level1, top_level2}, []string{ref1, ref2})
	if err != nil {
		t.Fatalf("No error expected when creating validator, got %v", err)
	}

	schemaID := "http://some_host.com/top1.json"
	if !v.HasSchema(schemaID) {
		t.Fatalf("%s schemaID is expected to be available", schemaID)
	}
	schemaID = "http://some_host.com/top2.json"
	if !v.HasSchema(schemaID) {
		t.Fatalf("%s schemaID is expected to be available", schemaID)
	}

	schemaID = "http://some_host.com/unknownscehma.json"
	if v.HasSchema(schemaID) {
		t.Fatalf("%s schemaID is not expected to be available", schemaID)
	}
}
