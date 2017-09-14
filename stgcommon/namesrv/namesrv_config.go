package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

const (
	separator               = string(os.PathSeparator)
	smartgoKVConfigFileName = "kvConfig.json"
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
	SmartgoHome  string `json:"smartgoHome"`
	KvConfigPath string `json:"kvConfigPath"`
}

type generator struct {
	Pkg    string
	Name   string
	Locale string
}

// NewNamesrvConfig 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func NewNamesrvConfig() NamesrvConfig {
	namesrvConfig := &DefaultNamesrvConfig{
		SmartgoHome:  getSmartGoHome(),
		KvConfigPath: GetKvConfigPath(),
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
		kvConfigPath := os.Getenv("GOPATH") + "/src/git.oschina.net/cloudzone/smartgo/stgregistry/start/conf/" + smartgoKVConfigFileName
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

// configPkg is the creator function, initiates kolpa with or without locale
// setting. The default locale setting is "en_US".
// Returns a generator type that will be used to call generator methods.
// http://www.cnblogs.com/vikings-blog/p/7131618.html
func configPkg(localeVar ...string) generator {
	newGenerator := generator{}
	if len(localeVar) > 0 {
		newGenerator.Locale = localeVar[0]
	} else {
		newGenerator.Locale = "conf"
	}

	newGenerator.Pkg = reflect.TypeOf(newGenerator).PkgPath()
	newGenerator.Locale = reflect.TypeOf(newGenerator).String()
	newGenerator.Name = reflect.TypeOf(newGenerator).Name()
	return newGenerator
}

// GetSmartGoHome 对外提供方法
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func (self *DefaultNamesrvConfig) GetSmartGoHome() string {
	return self.SmartgoHome
}

// GetKvConfigPath 对外提供方法
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func (self *DefaultNamesrvConfig) GetKvConfigPath() string {
	return self.KvConfigPath
}
