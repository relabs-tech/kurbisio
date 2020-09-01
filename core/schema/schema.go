package schema

import (
	"encoding/json"
	"fmt"
	"github.com/xeipuuv/gojsonschema"
)

// Validator is a utility to validate JSON object against a given schema
type Validator struct {
	schemaValidators map[string]*gojsonschema.Schema
}

// NewValidator creates a new Validator using schemas for the top level JSON schemas and refs
// for refs that may be referenced in the top level schemas. Top level schemas cannot reference each
// others. If a reference is mentioned, it can only be in the list of refs
func NewValidator(schemas []string, refs []string) (*Validator, error) {
	type schema struct {
		ID string `json:"$id"`
	}
	validator := Validator{schemaValidators: make(map[string]*gojsonschema.Schema)}
	for _, str := range schemas {
		s := schema{}
		err := json.Unmarshal([]byte(str), &s)
		if err != nil {
			return nil, fmt.Errorf("parse error '%v' in schema: '%s'", err, str)
		}
		if s.ID == "" {
			return nil, fmt.Errorf("schema does not contain $id: '%s'", str)
		}
		sl := gojsonschema.NewSchemaLoader()

		for _, ref := range refs {
			loader := gojsonschema.NewStringLoader(ref)
			err := sl.AddSchemas(loader)
			if err != nil {
				return nil, fmt.Errorf("cannot add ref %s %s", refs, err)
			}
		}
		schema, err := sl.Compile(gojsonschema.NewStringLoader(str))
		if err != nil {
			return nil, fmt.Errorf("cannot compile schema %s %s", s.ID, err)
		}
		validator.schemaValidators[s.ID] = schema
	}

	return &validator, nil
}

// HasSchema returns true if schemaID is known
func (v *Validator) HasSchema(schemaID string) bool {
	_, ok := v.schemaValidators[schemaID]
	return ok
}

// ValidateStruct validates the given json as a struct against schemaID. If no error is returned,
// then the passed json is valid
func (v *Validator) ValidateStruct(json interface{}, schemaID string) error {
	return v.validate(gojsonschema.NewGoLoader(json), schemaID)
}

// ValidateString validates the given json against schemaID. If no error is returned, then the
// passed json is valid
func (v *Validator) ValidateString(json, schemaID string) error {
	return v.validate(gojsonschema.NewStringLoader(json), schemaID)
}

// validate validates the given loader against schemaID. If no error is returned, then the passed json
// is valid
func (v *Validator) validate(loader gojsonschema.JSONLoader, schemaID string) error {

	schema, ok := v.schemaValidators[schemaID]
	if !ok {
		return fmt.Errorf("there is no schema %s ", schemaID)
	}

	result, err := schema.Validate(loader)
	if err != nil {
		return fmt.Errorf("cannot validate with schema %s %s", schemaID, err)
	}

	if !result.Valid() {
		err := "the document is not valid :\n"
		for _, e := range result.Errors() {
			err += fmt.Sprintf("- %s\n", e)
		}
		return fmt.Errorf(err)
	}
	return nil
}
