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
		value = val
	}
	f := Field{
		Name: key,
		Type: value,
	}
	return f
}

func createComplexField(val interface{}) interface{} {
	v := val.([]interface{})
	t := v[0].(string)
	tt, ok := v[1].(string)
	switch t {
	case "array":
		if ok {
			return map[string]string{"type": t, "items": tt}
		} else {
			if val, ok := v[1].([]interface{}); ok {
				return map[string]interface{}{"type": t, "items": createComplexField(val)}
			}
			return map[string]interface{}{"type": t, "items": createSchema(t, v[1].(map[string]interface{}))}
		}
	case "map":
		if ok {
			return map[string]string{"type": t, "values": tt}
		} else {
			return map[string]interface{}{"type": t, "values": createComplexField(v[1])}
		}
	//Structures
	default:
		return createSchema(t, v[1].(map[string]interface{}))
	}
}
