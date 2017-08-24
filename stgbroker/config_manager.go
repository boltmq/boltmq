package stgbroker

import (
	"fmt"
	"io/ioutil"
	"io"
	"os"
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
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Println("ReadFile: ", err.Error())
	}

	self.ConfigManager.Decode(bytes)
	return true
}

func (self *ConfigManagerExt) Persist() {
	jsonString := self.ConfigManager.Encode(true)
	if jsonString != "" {
		fileName := self.ConfigManager.ConfigFilePath()
		f, _:= os.OpenFile(fileName, os.O_APPEND, 0666)
		io.WriteString(f, jsonString)
		fmt.Println(jsonString,fileName)
		// TODO 写入文件
	}
}
