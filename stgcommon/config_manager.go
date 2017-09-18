package stgcommon

import (
	"io/ioutil"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

type IConfigManager interface {
	Encode(prettyFormat bool) string

	Decode(jsonString []byte)

	ConfigFilePath() string
}

type ConfigManager struct {
	IConfigManager
}

func (self *ConfigManager) load() bool {
	fileName := self.ConfigFilePath()
	bytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		logger.Infof("load %s Failed, and try to load backup file", fileName)
		return self.loadBak()
	}

	if len(bytes) == 0 {
		return self.loadBak()
	}

	self.Decode(bytes)
	logger.Infof("load %s Ok", fileName)

	return true

}

func (self *ConfigManager) loadBak() bool {
	fileName := self.ConfigFilePath()
	bytes, err := ioutil.ReadFile(fileName + ".bak")
	if err != nil {
		logger.Infof("load %s Failed", fileName)
		return false
	}

	if len(bytes) > 0 {
		self.Decode(bytes)
		logger.Infof("load %s Ok", fileName)
		return true
	}

	return true
}
