package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pquerna/ffjson/ffjson"
	"github.com/toolkits/file"
)

// ParseConfig 解析json文件到config结构体
// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-03-23
func ParseConfig(cfg string, config interface{}) error {
	if cfg == "" {
		return errors.New("use -c to specify configuration file")
	}

	if !file.IsExist(cfg) {
		var firstCfg = cfg
		// 此处为了兼容能够直接在idea上面利用web/etc默认配置文件目录 Add by ttx 2017-2-22 20:26:23
		cfg = "web/" + cfg
		if !file.IsExist(cfg) {
			return fmt.Errorf("config file: %s is not existent", firstCfg)
		}
	}

	configContent, err := ToTrimString(cfg)
	if err != nil {
		return fmt.Errorf("read config file: %s fail: %+v", cfg, err)
	}

	err = ffjson.Unmarshal([]byte(configContent), config)
	if err != nil {
		return fmt.Errorf("parse config file: %s fail: %+v", cfg, err)
	}

	return nil
}

// MaptoStruct map转换对象
// Author: jerrylou, <gunsluo@gmail.com>
// Since: 2017-03-23
func MaptoStruct(data map[string]interface{}, result interface{}) error {
	/*
		t := reflect.ValueOf(result).Elem()
		for k, v := range data {
			val := t.FieldByName(k)
			val.Set(reflect.ValueOf(v))
		}
	*/
	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = json.Unmarshal(buf, result)
	if err != nil {
		return err
	}

	return nil
}

// ToString read file content from filepath
func ToString(filePath string) (string, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ToTrimString read file content from filepath and replace space
func ToTrimString(filePath string) (string, error) {
	str, err := ToString(filePath)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(str), nil
}
