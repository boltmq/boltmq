package stgbroker

import (
	"fmt"
	"io/ioutil"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"sync"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
)

type ConfigManager interface {
	Encode(prettyFormat bool) string

	Decode(jsonString []byte)

	ConfigFilePath() string
}

type ConfigManagerExt struct {
	ConfigManager ConfigManager
	configLock    *sync.RWMutex
}

func NewConfigManagerExt(configManager ConfigManager) *ConfigManagerExt {
	return &ConfigManagerExt{
		ConfigManager: configManager,
	}
}

func (cme *ConfigManagerExt) Load() bool {
	fileName := cme.ConfigManager.ConfigFilePath()
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println("ReadFile: ", err.Error())
	}

	cme.ConfigManager.Decode(bytes)
	return true
}

func (cme *ConfigManagerExt) Persist() {
	defer utils.RecoveredFn()

	jsonString := cme.ConfigManager.Encode(true)
	if jsonString != "" {
		fileName := cme.ConfigManager.ConfigFilePath()
		stgcommon.String2File([]byte(jsonString), fileName)
	}
}
