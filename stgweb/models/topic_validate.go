package models

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"github.com/gunsluo/govalidator"
)

// Validate 参数验证
func (createTopic *CreateTopic) Validate() error {
	if err := utils.ValidateStruct(createTopic); err != nil {
		return createTopic.customizeValidationErr(err)
	}
	return nil
}

// 自定义错误提示
func (createTopic *CreateTopic) customizeValidationErr(err error) error {
	if _, ok := err.(*govalidator.UnsupportedTypeError); ok {
		return nil
	}

	for _, ve := range err.(govalidator.Errors) {
		e, ok := ve.(govalidator.Error)
		if !ok {
			continue
		}
		switch e.Name {
		case "clusterName":
			return fmt.Errorf("集群名称'%s'字段无效", e.Name)
		case "topic":
			return fmt.Errorf("topic名称'%s'字段无效", e.Name)
		}
	}

	return err
}

// Validate 参数验证
func (deleteTopic *DeleteTopic) Validate() error {
	if err := utils.ValidateStruct(deleteTopic); err != nil {
		return deleteTopic.customizeValidationErr(err)
	}
	return nil
}

// 自定义错误提示
func (deleteTopic *DeleteTopic) customizeValidationErr(err error) error {
	if _, ok := err.(*govalidator.UnsupportedTypeError); ok {
		return nil
	}

	for _, ve := range err.(govalidator.Errors) {
		e, ok := ve.(govalidator.Error)
		if !ok {
			continue
		}
		switch e.Name {
		case "clusterName":
			return fmt.Errorf("集群名称'%s'字段无效", e.Name)
		case "topic":
			return fmt.Errorf("topic名称'%s'字段无效", e.Name)
		}
	}

	return err
}

// Validate 参数验证
func (updateTopic *UpdateTopic) Validate() error {
	if err := utils.ValidateStruct(updateTopic); err != nil {
		return updateTopic.customizeValidationErr(err)
	}
	if !stgcommon.CheckIpAndPort(updateTopic.BrokerAddr) {
		return fmt.Errorf("broker地址brokerAddr=%s字段值无效", updateTopic.BrokerAddr)
	}
	return nil
}

// 自定义错误提示
func (updateTopic *UpdateTopic) customizeValidationErr(err error) error {
	if _, ok := err.(*govalidator.UnsupportedTypeError); ok {
		return nil
	}

	for _, ve := range err.(govalidator.Errors) {
		e, ok := ve.(govalidator.Error)
		if !ok {
			continue
		}
		switch e.Name {
		case "clusterName":
			return fmt.Errorf("'%s'字段无效", e.Name)
		case "topic":
			return fmt.Errorf("'%s'字段无效", e.Name)
		case "brokerAddr":
			return fmt.Errorf("'%s'字段无效", e.Name)
		case "writeQueueNums":
			return fmt.Errorf("'%s'字段无效、最小值为8", e.Name)
		case "readQueueNums":
			return fmt.Errorf("'%s'字段无效、最小值为8", e.Name)
		}
	}

	return err
}
