package api

import (
	"reflect"
	"testing"
)

func TestSchemaForTypePreservesRepeatedSiblingStructs(t *testing.T) {
	type leaf struct {
		Name  string  `json:"name"`
		Score float64 `json:"score,omitempty"`
	}
	type pair struct {
		First  leaf  `json:"first"`
		Second leaf  `json:"second"`
		Third  *leaf `json:"third,omitempty"`
	}

	schema := schemaForType(reflect.TypeOf(pair{}), make(map[reflect.Type]bool))
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("pair schema has no properties: %#v", schema)
	}
	for _, fieldName := range []string{"first", "second", "third"} {
		fieldSchema, ok := props[fieldName].(map[string]any)
		if !ok {
			t.Fatalf("pair.%s schema missing: %#v", fieldName, props[fieldName])
		}
		leafProps, ok := fieldSchema["properties"].(map[string]any)
		if !ok {
			t.Fatalf("pair.%s collapsed without properties: %#v", fieldName, fieldSchema)
		}
		score, ok := leafProps["score"].(map[string]any)
		if !ok || score["type"] != "number" {
			t.Fatalf("pair.%s.score = %#v; want type number", fieldName, leafProps["score"])
		}
	}

	// Truly recursive types must still terminate without stack overflow.
	type node struct {
		Label string `json:"label"`
		Next  *node  `json:"next,omitempty"`
	}
	nodeSchema := schemaForType(reflect.TypeOf(node{}), make(map[reflect.Type]bool))
	nodeProps, ok := nodeSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("node schema missing properties: %#v", nodeSchema)
	}
	nextSchema, ok := nodeProps["next"].(map[string]any)
	if !ok || nextSchema["type"] != "object" {
		t.Fatalf("node.next = %#v; want fallback object", nodeProps["next"])
	}
}
