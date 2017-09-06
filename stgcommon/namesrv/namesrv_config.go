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
type NamesrvConfig struct {
	SmartGoHome  string
	KvConfigPath string
}

// GetSmartGoHome
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func GetSmartGoHome() string {
	smartGoHome := strings.TrimSpace(os.Getenv(stgcommon.CLOUDMQ_HOME_PROPERTY))
	if smartGoHome == "" {
		return stgcommon.SMARTGO_HOME_ENV
	}
	return smartGoHome
}

func GetKvConfigPath() string {
	workDir, _ := os.Getwd()
	format := workDir + separator + "stgregistry" + separator + "kvConfig.json"
	kvConfigPath := filepath.ToSlash(format) // 将workDir中平台相关的路径分隔符转换为'/'
	// example E:/source/src/git.oschina.net/cloudzone/smartgo/stgcommon/namesrv/stgregistry/kvConfig.json
	return kvConfigPath
}
