package stgbroker

import (
	"fmt"
	"io/ioutil"
)

type ConfigManager interface {
	Encode(prettyFormat bool) string

	Decode(jsonString []byte)

	ConfigFilePath() string
}

type ConfigManagerExt struct {
	ConfigManager ConfigManager
	// TODO log
}

func NewConfigManagerExt(configManager ConfigManager) *ConfigManagerExt {
	return &ConfigManagerExt{
		ConfigManager: configManager,
	}
}

func (self *ConfigManagerExt) Load() bool {
	fileName := self.ConfigManager.ConfigFilePath()
	fmt.Println(fileName)
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println("ReadFile: ", err.Error())
	}
	self.ConfigManager.Decode(bytes)
	return true
}

func (self *ConfigManagerExt) Persist() {
	jsonString := self.ConfigManager.Encode(true)
	if jsonString == "" {
		fileName := self.ConfigManager.ConfigFilePath()
		fmt.Println(jsonString,fileName)
		// TODO 写入文件
	}
}
