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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/boltmq/common/utils/encoding"
	"github.com/boltmq/common/utils/system"
	"github.com/flosch/pongo2"
	"github.com/imdario/mergo"
	"github.com/juju/errors"
)

const (
	defaultBrokerPermission = 6
	defaultTopicQueueNums   = 8
	MASTER_ID               = 0
)

// ParseConfig 解析配置文件
func ParseConfig(path string) (*Config, error) {
	var (
		cfg Config
	)

	path = getConfigEnvValue(path, envBoltmqBrokerConfigPath)
	if err := encoding.DecodeToml(path, &cfg); err != nil {
		return nil, err
	}

	if err := mergeConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

var defaultConfig = &Config{
	Cluster: ClusterConfig{
		Name:         "BoltMQCluster",
		BrokerId:     MASTER_ID,
		BrokerName:   "broker-node",
		BrokerRole:   "SYNC_MASTER",
		HaServerIP:   defaultLocalAddress(),
		NameSrvAddrs: getNameSrvAddrs(),
	},
	Broker: BrokerConfig{
		IP:                                 defaultLocalAddress(),
		Port:                               11911,
		DeleteWhen:                         4,
		Permission:                         defaultBrokerPermission,
		DefaultTopicQueueNums:              defaultTopicQueueNums,
		AutoCreateTopicEnable:              false,
		ClusterTopicEnable:                 true,
		BrokerTopicEnable:                  true,
		AutoCreateSubscriptionGroup:        true,
		FlushConsumerOffsetInterval:        5000,
		FlushConsumerOffsetHistoryInterval: 60000,
		RejectTransactionMessage:           false,
		FetchNameSrvAddrByAddressServer:    false,
		SendThreadPoolQueueCapacity:        100000,
		PullThreadPoolQueueCapacity:        100000,
		FilterServerNums:                   0,
		LongPollingEnable:                  true,
		ShortPollingTimeMills:              1000,
		NotifyConsumerIdsChangedEnable:     true,
		OffsetCheckInSlave:                 true,
		HaMasterAddress:                    "",
	},
	Store: StoreConfig{
		RootDir:          defaultRootDir(),
		FileReservedTime: 48,
		FlushDiskType:    "SYNC_FLUSH",
	},
	Log: LogConfig{
		CfgFilePath: "etc/seelog.xml",
	},
}

func mergeConfig(cfg *Config) error {
	if err := mergo.Merge(cfg, defaultConfig); err != nil {
		return err
	}

	//对路径进行修正
	cfg.Store.RootDir = fixPath(cfg.Store.RootDir)
	cfg.Log.CfgFilePath = fixPath(cfg.Log.CfgFilePath)
	return nil
}

func fixPath(path string) string {
	uhome := system.Home()
	if uhome == "" {
		return path
	}

	tpl, err := pongo2.FromString(path)
	if err != nil {
		return path
	}

	// 替换Home
	out, err := tpl.Execute(pongo2.Context{"HOME": uhome})
	if err != nil {
		return path
	}

	return filepath.FromSlash(out)
}

func defaultRootDir() string {
	uhome := system.Home()
	return fmt.Sprintf("%s%cstore", uhome, os.PathSeparator)
}

func getNameSrvAddrs() []string {
	vals := getConfigEnvValue("", envNameSrvAddrs)
	return strings.Split(vals, ";")
}

func defaultLocalAddress() string {
	if laddr, err := localAddress(); err == nil {
		return laddr
	}

	return "127.0.0.1"
}

func localAddress() (laddr string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return laddr, err
	}

	for _, addr := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil && !isIntranetIpv4(ipnet.IP.String()) {
				return ipnet.IP.String(), nil
			}
		}
	}

	return "", errors.Errorf("<none>")
}

func isIntranetIpv4(ip string) bool {
	//if strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "169.254.") {
	if strings.HasPrefix(ip, "169.254.") {
		return true
	}
	return false
}
