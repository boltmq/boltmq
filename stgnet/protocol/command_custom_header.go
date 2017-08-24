package protocol

import (
	"reflect"
	"strconv"

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
func decodeCommandCustomHeader(extFields map[string]string, commandCustomHeader CommandCustomHeader) error {
	for k, v := range extFields {
		err := reflectSturctSetField(commandCustomHeader, k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// 支持string int8 int16 int int32 int64 uint8 uint16 uint32 uint64 bool，非string类型将进行转换
func reflectSturctSetField(obj interface{}, name string, value string) error {
	structValue := reflect.ValueOf(obj).Elem()
	structFieldValue := structValue.FieldByName(name)

	if !structFieldValue.IsValid() {
		return errors.Errorf("No such field: %s in obj", name)
	}

	if !structFieldValue.CanSet() {
		return errors.Errorf("Cannot set %s field value", name)
	}

	structFieldType := structFieldValue.Type()
	switch structFieldType.Kind() {
	case reflect.String:
		structFieldValue.Set(reflect.ValueOf(value))
	case reflect.Int32:
		ival, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(int32(ival)))
	case reflect.Int64:
		ival, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(ival))
	case reflect.Int:
		ival, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(int(ival)))
	case reflect.Int16:
		ival, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(int16(ival)))
	case reflect.Int8:
		ival, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(int8(ival)))
	case reflect.Uint16:
		ival, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(uint16(ival)))
	case reflect.Uint8:
		ival, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(uint8(ival)))
	case reflect.Uint32:
		ival, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(uint32(ival)))
	case reflect.Uint64:
		ival, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(ival))
	case reflect.Bool:
		bval, err := strconv.ParseBool(value)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.Set(reflect.ValueOf(bval))
	default:
		return errors.Errorf("Provided value type didn't match obj field type")
	}

	return nil
}
