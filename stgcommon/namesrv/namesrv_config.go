package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"os"
	"path/filepath"
	"strings"
)

const (
	separator               = string(os.PathSeparator)
	smartgoKVConfigFileName = "smartgoKVConfig.toml"
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

// NewNamesrvConfig 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func NewNamesrvConfig() NamesrvConfig {
	namesrvConfig := &DefaultNamesrvConfig{
		smartgoHome:  getSmartGoHome(),
		kvConfigPath: GetKvConfigPath(),
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

// GetKvConfigPath 获得KV配置文件路径
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func GetKvConfigPath() string {
	// 获取程序运行路径
	workDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	format := workDir + separator + smartgoKVConfigFileName
	kvConfigPath := filepath.ToSlash(format) // 将workDir中平台相关的路径分隔符转换为'/'
	if exists, _ := validateExists(kvConfigPath); !exists {
		// 特别标注，配合IDE开发
		// workDirPath, _ := os.Getwd()
		kvConfigPath = "../../conf/" + string(smartgoKVConfigFileName)
		fmt.Printf("ide.kvConfigPath=%s\n", kvConfigPath)
		return kvConfigPath
	}

	fmt.Printf("namesrvConfig.kvConfigPath=%s\n", kvConfigPath)
	return kvConfigPath
}

// validateExists 校验文件或文件夹是否存在
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/13
func validateExists(fileFullPath string) (bool, error) {
	_, err := os.Stat(fileFullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
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
