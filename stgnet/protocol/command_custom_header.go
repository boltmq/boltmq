package protocol

import (
	"reflect"

	"github.com/go-errors/errors"
)

// CommandCustomHeader:头信息接口
// Author: yintongqiang
// Since:  2017/8/16

type CommandCustomHeader interface {
	CheckFields() error
}

// DecodeCommandCustomHeader 将extFields转为struct
// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-08-24
func DecodeCommandCustomHeader(extFields map[string]string, commandCustomHeader CommandCustomHeader) error {
	for k, v := range extFields {
		err := reflectSturctSetField(commandCustomHeader, k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func reflectSturctSetField(obj interface{}, name string, value interface{}) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return errors.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return errors.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	val := reflect.ValueOf(value)
	if structFieldType != val.Type() {
		return errors.Errorf("Provided value type didn't match obj field type")
	}

	structFieldValue.Set(val)
	return nil
}
