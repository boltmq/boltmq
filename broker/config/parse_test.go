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
package config

import (
	"testing"
)

func TestParseConfig(t *testing.T) {
	cfg, err := ParseConfig("../etc/broker_sample.toml")
	if err != nil {
		t.Errorf("ParseConfig:%s", err)
		return
	}

	if cfg.Cluster.Name != defaultConfig.Cluster.Name {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Broker.DeleteWhen != defaultConfig.Broker.DeleteWhen {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Store.FileReservedTime != defaultConfig.Store.FileReservedTime {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Store.FlushDiskType != defaultConfig.Store.FlushDiskType {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Broker.AutoCreateTopicEnable != defaultConfig.Broker.AutoCreateTopicEnable {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Cluster.BrokerName != defaultConfig.Cluster.BrokerName {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Cluster.BrokerRole != defaultConfig.Cluster.BrokerRole {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Store.RootDir != defaultRootDir() {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Log.CfgFilePath != defaultConfig.Log.CfgFilePath {
		t.Errorf("ParseConfig failed")
		return
	}
}

func TestParseConfigError(t *testing.T) {
	_, err := ParseConfig("")
	if err == nil {
		t.Errorf("ParseConfig:%s", err)
		return
	}
}
