package kafka

import (
	"encoding/binary"
	"reflect"
	"sort"
)

type Schema struct {
	Type   string  `json:"type"`
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

type Field struct {
	Name string      `json:"name"`
	Type interface{} `json:"type"`
}

type ListField struct {
	Type  string      `json:"type"`
	Items interface{} `json:"items"`
}

type MapField struct {
	Type   string      `json:"type"`
	Values interface{} `json:"values"`
}

func createSchema(name string, fieldsMap map[string]interface{}) Schema {
	fields := make([]Field, 0, len(fieldsMap))
	for key, val := range fieldsMap {
		fields = append(fields, createField(key, val))
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	return Schema{
		Type:   "record",
		Name:   name,
		Fields: fields,
	}
}

func createField(key string, val interface{}) Field {
	var value interface{}
	if reflect.TypeOf(val).Kind() == reflect.Slice {
		value = createComplexField(val)
	} else {
		switch val {
		case "int", "int8", "int32":
			value = "int"
		case "int64":
			value = "long"
		case "bool":
			value = "boolean"
		default:
			value = val
		}

	}
	f := Field{
		Name: key,
		Type: value,
	}
	return f
}

/*
Creates an Avro complex field (record, map, array) using the map[string]interface{} with the names and types reflection
Enum is currently not supported here
If the type is an array returns a ListField with items as primitive type or a complex embedded type
If the type is map returns a MapField with values as primitive type or a complex embedded type
If the type is any structure, returns a record type, represented on the schema
*/
func createComplexField(val interface{}) interface{} {
	v := val.([]interface{})
	t := v[0].(string)
	tt, ok := v[1].(string)
	switch t {
	case "array":
		return creatListField(ok, t, v[1])
	case "map":
		var values interface{}
		if ok {
			values = tt
		} else {
			values = createComplexField(v[1])
		}
		return MapField{
			Type:   t,
			Values: values,
		}
	//Structures
	default:
		return createSchema(t, v[1].(map[string]interface{}))
	}
}

func creatListField(ok bool, t string, value interface{}) ListField {
	var items interface{}
	if ok {
		items = value.(string)

	} else {
		if val, ok := value.([]interface{}); ok {
			items = createComplexField(val)
		} else {
			items = createSchema(t, value.(map[string]interface{}))
		}
	}
	return ListField{
		Type:  t,
		Items: items,
	}
}

// AvroEncoder encodes schemaId and Avro message.
type AvroEncoder struct {
	SchemaID int
	Content  []byte
}

// Notice: the Confluent schema registry has special requirements for the Avro serialization rules,
// not only need to serialize the specific content, but also attach the Schema ID and Magic Byte.
// Ref: https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
func (a *AvroEncoder) Encode() ([]byte, error) {
	var binaryMsg []byte
	// Confluent serialization format version number; currently always 0.
	binaryMsg = append(binaryMsg, byte(0))
	// 4-byte schema ID as returned by Schema Registry
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(a.SchemaID))
	binaryMsg = append(binaryMsg, binarySchemaId...)
	// Avro serialized data in Avro's binary encoding
	binaryMsg = append(binaryMsg, a.Content...)
	return binaryMsg, nil
}

// Length of schemaId and Content.
func (a *AvroEncoder) Length() int {
	return 5 + len(a.Content)
}
