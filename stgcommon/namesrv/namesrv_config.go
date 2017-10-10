package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"os"
	"path/filepath"
	"strings"
)

const (
	childDir  = "namesrv"
	separator = string(os.PathSeparator)
	cfgName   = "kvConfig.json"
)

// DefaultNamesrvConfig 默认Namesrv配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
type NamesrvConfig struct {
	smartgoHome  string
	kvConfigPath string
}

// NewNamesrvConfig 初始化配置项
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func NewNamesrvConfig() *NamesrvConfig {
	cfg := &NamesrvConfig{
		smartgoHome:  getSmartGoHome(),
		kvConfigPath: getKvConfigPath(),
	}
	return cfg
}

// GetSmartGoHome 获取SmartGoHome配置目录
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *NamesrvConfig) GetSmartGoHome() string {
	return self.smartgoHome
}

// GetKvConfigPath 获取Namesrv配置文件的完整路径
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func (self *NamesrvConfig) GetKvConfigPath() string {
	return self.kvConfigPath
}

// GetKvConfigDir 获取Namesrv配置文件完整路径
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func (self *NamesrvConfig) GetKvConfigDir() string {
	return filepath.Dir(self.kvConfigPath)
}

// GetKvConfigName 获取Namesrv配置文件名
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func (self *NamesrvConfig) GetKvConfigName() string {
	return cfgName
}

// ToString 打印Namesrv基础配置信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func (self *NamesrvConfig) ToString() string {
	return fmt.Sprintf("namesrv cfg [smartgoHome=%s, kvConfigPath=%s]", self.smartgoHome, self.kvConfigPath)
}

// getSmartGoHome 获得默认配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func getSmartGoHome() string {
	smartGoHome := strings.TrimSpace(stgcommon.GetSmartGoHome())
	if smartGoHome == "" {
		return stgcommon.GetUserHomeDir()
	}
	return smartGoHome
}

// getKvConfigPath 获得KV配置文件路径
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func getKvConfigPath() string {
	kvConfigPath := filepath.ToSlash(stgcommon.GetUserHomeDir() + separator + childDir + separator + cfgName)
	return kvConfigPath
}
