package stgbroker

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"github.com/toolkits/file"
	"sync"
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
	if !file.IsExist(fileName) {
		// 第一次启动服务，如果topic.json、subscriptionGroup.json、consumerOffset.json之类的文件不存在，则创建之
		ok, err := stgcommon.CreateFile(fileName)
		if err != nil {
			fmt.Printf("create %s failed. err: %s \n", fileName, err.Error())
			return false
		}
		if !ok {
			fmt.Printf("create %s failed, unknown reason. \n", fileName)
			return false
		}
		fmt.Printf("create %s successful. \n", fileName)
	}

	buf, err := file.ToBytes(fileName)
	if err != nil {
		fmt.Println("read file err: %s", err.Error())
		return false
	}

	cme.ConfigManager.Decode(buf)
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
