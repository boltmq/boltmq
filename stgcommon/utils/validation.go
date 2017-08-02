package utils

import (
	"github.com/gunsluo/govalidator"
	"github.com/gunsluo/govalidator/custom"
)

func init() {
	govalidator.CustomTypeTagMap.Set("max", govalidator.CustomTypeValidator(custom.MaxCustomTypeTagFn))
	govalidator.CustomTypeTagMap.Set("min", govalidator.CustomTypeValidator(custom.MinCustomTypeTagFn))
	govalidator.CustomTypeTagMap.Set("gt", govalidator.CustomTypeValidator(custom.GtCustomTypeTagFn))
	govalidator.CustomTypeTagMap.Set("gte", govalidator.CustomTypeValidator(custom.MinCustomTypeTagFn))
	govalidator.CustomTypeTagMap.Set("lt", govalidator.CustomTypeValidator(custom.LtCustomTypeTagFn))
	govalidator.CustomTypeTagMap.Set("lte", govalidator.CustomTypeValidator(custom.MaxCustomTypeTagFn))
}

// ValidateStruct 结构体验证
func ValidateStruct(obj interface{}) error {

	if res, err := govalidator.ValidateStruct(obj); !res {
		return err
	}

	return nil
}

// ValidateVar 变量验证
func ValidateVar(field string, tag string) (bool, error) {
	return govalidator.ValidateVar(field, tag)
}
