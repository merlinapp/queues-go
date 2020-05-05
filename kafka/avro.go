package kafka

import (
	"reflect"
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
