package queuesgo

import "reflect"

func ValidateType(objType interface{}) bool {
	t := reflect.TypeOf(objType)
	if t.Kind() == reflect.Ptr {
		return objType != nil
	}
	return t.Kind() == reflect.Struct || t.Kind() == reflect.Map

}

func GetName(val interface{}) string {
	if t := reflect.TypeOf(val); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

/*
Returns a map string key, val interface with
key: the name of the field, if it have a json tag it will take the tag name
value: the type of the field if is primitive (string, int, long. bool...) as string
if is a complex type (structure, map, slice) the value will have an slice with 2 positions
the first position indicates the type, array for slices, map or the name of the structure
the second position indicates the type of the slice or map, a map with the previous rules for embedded structures
with maps you can assume a key string as it is the most usual, but for maps there is an extra position with the key type
*/
func GetFields(val interface{}) map[string]interface{} {
	v := reflect.Indirect(reflect.ValueOf(val))
	fields := make(map[string]interface{}, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			fields[jsonTag] = getType(v.Field(i).Type())
		} else {
			fields[field.Name] = getType(v.Field(i).Type())
		}
	}
	return fields
}

func getType(t reflect.Type) interface{} {
	switch t.Kind() {
	case reflect.Struct:
		val := reflect.New(t).Interface()
		return []interface{}{GetName(val), GetFields(val)}
	case reflect.Slice:
		return []interface{}{"array", getType(t.Elem())}
	case reflect.Map:
		return []interface{}{"map", getType(t.Elem()), getType(t.Key())}
	default:
		return t.Name()
	}
}

func ValidateRegisteredType(obj interface{}, regType reflect.Type) bool {
	if !ValidateType(obj) {
		return false
	}
	t := reflect.TypeOf(obj)
	switch {
	case t == regType:
		return true
	case t.Kind() == reflect.Ptr && (regType.Kind() == reflect.Struct || regType.Kind() == reflect.Map):
		if t == reflect.PtrTo(regType) {
			return true
		}
	case regType.Kind() == reflect.Ptr && (t.Kind() == reflect.Struct || t.Kind() == reflect.Map):
		if regType == reflect.PtrTo(t) {
			return true
		}
	}
	return false
}
