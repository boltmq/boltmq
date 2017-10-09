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
	structValue := reflect.ValueOf(commandCustomHeader).Elem()

	for k, v := range extFields {
		err := reflectSturctSetField(structValue, firstLetterToUpper(k), v)
		if err != nil {
			return err
		}
	}

	return nil
}

// 支持string int8 int16 int int32 int64 uint8 uint16 uint32 uint64 bool，非string类型将进行转换
func reflectSturctSetField(structValue reflect.Value, name string, value string) error {
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
		structFieldValue.SetString(value)
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Int:
		ival, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.SetInt(ival)
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		ival, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.SetUint(ival)
	case reflect.Bool:
		bval, err := strconv.ParseBool(value)
		if err != nil {
			return errors.Wrap(err, 0)
		}
		structFieldValue.SetBool(bval)
	default:
		return errors.Errorf("Provided value type didn't match obj field type")
	}

	return nil
}

// 将CommandCustomHeader转为map
func encodeCommandCustomHeader(commandCustomHeader CommandCustomHeader) map[string]string {
	var value string
	structValue := reflect.ValueOf(commandCustomHeader).Elem()

	extFields := make(map[string]string)
	for i := 0; i < structValue.NumField(); i++ {
		f := structValue.Field(i)
		t := structValue.Type().Field(i)

		switch f.Type().Kind() {
		case reflect.String:
			extFields[t.Name] = f.String()
		case reflect.Int32:
			value = strconv.FormatInt(f.Int(), 10)
			extFields[t.Name] = value
		case reflect.Int64:
			value = strconv.FormatInt(f.Int(), 10)
			extFields[t.Name] = value
		case reflect.Int:
			value = strconv.FormatInt(f.Int(), 10)
			extFields[t.Name] = value
		case reflect.Int16:
			value = strconv.FormatInt(f.Int(), 10)
			extFields[t.Name] = value
		case reflect.Int8:
			value = strconv.FormatInt(f.Int(), 10)
			extFields[t.Name] = value
		case reflect.Uint16:
			value = strconv.FormatUint(f.Uint(), 10)
			extFields[t.Name] = value
		case reflect.Uint8:
			value = strconv.FormatUint(f.Uint(), 10)
			extFields[t.Name] = value
		case reflect.Uint32:
			value = strconv.FormatUint(f.Uint(), 10)
			extFields[t.Name] = value
		case reflect.Uint64:
			value = strconv.FormatUint(f.Uint(), 10)
			extFields[t.Name] = value
		case reflect.Bool:
			value = strconv.FormatBool(f.Bool())
			extFields[t.Name] = value
		default:
		}
	}

	return extFields
}

// 首字母大写
func firstLetterToUpper(s string) string {
	if len(s) > 0 {
		b := []byte(s)
		if b[0] >= 'a' && b[0] <= 'c' {
			b[0] = b[0] - byte(32)
			s = string(b)
		}
	}

	return s
}

func isExistFieldInStruct(structValue reflect.Value, name string) bool {
	for i := 0; i < structValue.NumField(); i++ {
		t := structValue.Type().Field(i)
		if t.Name == name {
			return true
		}
	}

	return false
}
