package queuesgo

import "reflect"

func ValidateType(objType interface{}) bool {
	t := reflect.TypeOf(objType)
	if t.Kind() == reflect.Ptr {
		return objType != nil
	}
	return t.Kind() == reflect.Struct || t.Kind() == reflect.Map

}

func GetType(val interface{}) string {
	if t := reflect.TypeOf(val); t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	} else {
		return t.Name()
	}
}

func GetFields(val interface{}) map[string]string {
	v := reflect.Indirect(reflect.ValueOf(val))
	fields := make(map[string]string, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag != "" {
			fields[jsonTag] = v.Field(i).Type().Name()
		} else {
			fields[field.Name] = v.Field(i).Type().Name()
		}
	}
	return fields
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
