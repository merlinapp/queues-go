package queuesgo

import "reflect"

func ValidateType(objType interface{}) bool {
	t := reflect.TypeOf(objType)
	if t.Kind() == reflect.Ptr {
		return objType != nil
	}
	return t.Kind() == reflect.Struct || t.Kind() == reflect.Map

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