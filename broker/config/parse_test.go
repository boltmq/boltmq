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

	if cfg.Cluster.ClusterName != "BoltMQCluster" {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Cluster.DeleteWhen != 4 {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Cluster.FileReservedTime != 48 {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Cluster.FlushDiskType != "SYNC_FLUSH" {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Cluster.AutoCreateTopicEnable != false {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Cluster.BrokerName != "broker-node" {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Cluster.BrokerRole != "SYNC_MASTER" {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Store.RootDir != defaultRootDir() {
		t.Errorf("ParseConfig failed")
		return
	}

	if cfg.Log.CfgFilePath != "etc/seelog.xml" {
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
