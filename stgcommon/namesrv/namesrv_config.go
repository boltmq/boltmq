package namesrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"os"
	"os/user"
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
	smartGoHome := strings.TrimSpace(os.Getenv(stgcommon.SMARTGO_HOME_ENV))
	if smartGoHome == "" {
		rootDir, err := user.Current()
		if err != nil {
			fmt.Printf("get default smartGoHomeEnv err: %s \n", err.Error())
			return "" //stgcommon.SMARTGO_HOME_ENV
		}
		return rootDir.HomeDir
	}
	return smartGoHome
}

// getKvConfigPath 获得KV配置文件路径
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/8
func getKvConfigPath() string {
	rootDir, err := user.Current()
	if err != nil {
		msg := fmt.Sprintf("get rootDir err: %s \n", err.Error())
		fmt.Printf(msg)
		panic(msg)
	}
	kvConfigPath := filepath.ToSlash(rootDir.HomeDir + separator + childDir + separator + cfgName)
	return kvConfigPath

	//// 获取程序运行路径
	//workDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	//format := workDir + separator + smartgoKVConfigFileName
	//kvConfigPath := filepath.ToSlash(format) // 将workDir中平台相关的路径分隔符转换为'/'
	//if exists, _ := validateExists(kvConfigPath); !exists {
	//	// 特别标注，配合IDE开发
	//	kvConfigPath := os.Getenv("GOPATH") + "/src" + projectPath + moduleName + "/" + smartgoKVConfigFileName
	//	fmt.Printf("ide.kvConfigPath=%s\n", kvConfigPath)
	//	return kvConfigPath
	//}
	//return kvConfigPath
}
