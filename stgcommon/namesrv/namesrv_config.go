package namesrv

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"os"
	"path/filepath"
	"strings"
)

const (
	separator = string(os.PathSeparator)
)

// NamesrvConfig namesrv配置项
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
type NamesrvConfig interface {
	GetSmartGoHome() string
	GetKvConfigPath() string
}

// DefaultNamesrvConfig 默认Namesrv配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
type DefaultNamesrvConfig struct {
	smartgoHome  string `json:"smartgoHome"`
	kvConfigPath string `json:"kvConfigPath"`
}

// NewDefaultNamesrvConfig 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func NewDefaultNamesrvConfig() *DefaultNamesrvConfig {
	namesrvConfig := &DefaultNamesrvConfig{
		smartgoHome:  getSmartGoHome(),
		kvConfigPath: getKvConfigPath(),
	}

	return namesrvConfig
}

// NewNamesrvConfig 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func NewNamesrvConfig() NamesrvConfig {
	namesrvConfig := &DefaultNamesrvConfig{
		smartgoHome:  getSmartGoHome(),
		kvConfigPath: getKvConfigPath(),
	}

	return namesrvConfig
}

// getSmartGoHome 获得默认配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func getSmartGoHome() string {
	smartGoHome := strings.TrimSpace(os.Getenv(stgcommon.CLOUDMQ_HOME_PROPERTY))
	if smartGoHome == "" {
		return stgcommon.SMARTGO_HOME_ENV
	}
	return smartGoHome
}

// getKvConfigPath 获得KV配置文件路径
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func getKvConfigPath() string {
	workDir, _ := os.Getwd()
	format := workDir + separator + "stgregistry" + separator + "kvConfig.json"
	kvConfigPath := filepath.ToSlash(format) // 将workDir中平台相关的路径分隔符转换为'/'
	return kvConfigPath
}

// GetSmartGoHome 对外提供方法
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultNamesrvConfig) GetSmartGoHome() string {
	return self.smartgoHome
}

// GetKvConfigPath 对外提供方法
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func (self *DefaultNamesrvConfig) GetKvConfigPath() string {
	return self.kvConfigPath
}
