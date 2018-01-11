// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"strings"
	"sync"

	"github.com/boltmq/boltmq/common"
	"github.com/boltmq/common/logger"
	"github.com/toolkits/file"
)

// configManager 配置管理
type configManager interface {
	encode(prettyFormat bool) string

	decode(jsonString []byte)

	configFilePath() string
}

// configManagerLoader 配置管理加载器
type configManagerLoader struct {
	cfgManager configManager
	lock       sync.RWMutex
}

func newConfigManagerLoader(cfgManager configManager) *configManagerLoader {
	return &configManagerLoader{
		cfgManager: cfgManager,
	}
}

func (cml *configManagerLoader) load() bool {
	filePath := cml.cfgManager.configFilePath()
	if !file.IsExist(filePath) {
		// 第一次启动服务，如果topic.json、subscriptionGroup.json、consumerOffset.json之类的文件不存在，则创建之
		ok, err := common.CreateNullFile(filePath)
		if err != nil {
			logger.Errorf("create %s failed. err: %s.", filePath, err)
			return false
		}
		if !ok {
			logger.Errorf("create %s failed, unknown reason.", filePath)
			return false
		}
		logger.Infof("create %s success.", filePath)
	}

	buf, err := file.ToBytes(filePath)
	if err != nil {
		logger.Errorf("read file err: %s.", err)
		return false
	}

	cml.cfgManager.decode(buf)
	return true
}

func (cml *configManagerLoader) persist() {
	cml.lock.Lock()
	defer cml.lock.Unlock()

	buf := strings.TrimSpace(cml.cfgManager.encode(true))
	if buf == "" {
		logger.Warnf("configManagerLoader nothing to persist.")
		return
	}

	filePath := cml.cfgManager.configFilePath()
	err := common.String2File([]byte(buf), filePath)
	if err != nil {
		logger.Errorf("persist string to file, %s.", err)
	}
}
