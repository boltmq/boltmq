package utils

import (
	"errors"
	"fmt"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/toolkits/file"
	"strings"
)

// ParseConfig 解析json文件到config结构体
func ParseConfig(cfg string, config interface{}) error {
	if cfg == "" {
		return errors.New("use -c to specify configuration file")
	}

	if !file.IsExist(cfg) {
		var firstCfg = cfg
		// 此处为了兼容能够直接在idea上面利用start/etc默认配置文件目录
		cfg = "start/" + cfg
		if !file.IsExist(cfg) {
			return fmt.Errorf("config file: %s is not existent", firstCfg)
		}
	}

	content, err := ToTrimString(cfg)
	if err != nil {
		return fmt.Errorf("read config file: %s fail: %+v", cfg, err)
	}

	err = ffjson.Unmarshal([]byte(content), config)
	if err != nil {
		return fmt.Errorf("parse config file: %s fail: %+v", cfg, err)
	}

	return nil
}

// ToTrimString read file content from filepath and replace space
func ToTrimString(filePath string) (content string, err error) {
	content, err = file.ToString(filePath)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(content), nil
}
