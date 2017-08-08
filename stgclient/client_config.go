package stgclient

import (
	"os"
	"runtime"
	"fmt"
	"net"
	"strings"
	"strconv"
)
// 发送状态枚举
// Author: yintongqiang
// Since:  2017/8/8

type ClientConfig struct {
	NamesrvAddr                   string
	InstanceName                  string
	ClientIP                      string
	ClientCallbackExecutorThreads int
	PollNameServerInterval        int
	HeartbeatBrokerInterval       int
	PersistConsumerOffsetInterval int
}

func NewClientConfig(namesrvAddr string) *ClientConfig {
	instanceName := os.Getenv("smartgo.client.name")
	if strings.EqualFold(instanceName, "") {
		instanceName = "DEFAULT"
	}
	return &ClientConfig{NamesrvAddr:namesrvAddr,
		InstanceName: instanceName,
		ClientIP:getLocalAddress(),
		ClientCallbackExecutorThreads:runtime.NumCPU(),
		PollNameServerInterval:1000 * 30,
		HeartbeatBrokerInterval:1000 * 30,
		PersistConsumerOffsetInterval:1000 * 5}
}

func getLocalAddress() string {
	adds, _ := net.InterfaceAddrs()
	for _, address := range adds {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}

		}
	}
	return ""
}

func (client *ClientConfig) BuildMQClientId() string {
	return fmt.Sprintf("%s@%s", client.ClientIP, client.InstanceName)
}

func (client *ClientConfig) ChangeInstanceNameToPID() {
	if strings.EqualFold(client.InstanceName, "DEFAULT") {
		client.InstanceName = strconv.Itoa(os.Getpid())
	}
}

func (client *ClientConfig) CloneClientConfig() *ClientConfig {
	return &ClientConfig{NamesrvAddr:client.NamesrvAddr,
		InstanceName: client.InstanceName,
		ClientIP:client.ClientIP,
		ClientCallbackExecutorThreads:client.ClientCallbackExecutorThreads,
		PollNameServerInterval:client.PollNameServerInterval,
		HeartbeatBrokerInterval:client.HeartbeatBrokerInterval,
		PersistConsumerOffsetInterval:client.PersistConsumerOffsetInterval}
}

func (client *ClientConfig) ResetClientConfig(cc*ClientConfig) {
	client.NamesrvAddr = cc.NamesrvAddr;
	client.ClientIP = cc.ClientIP;
	client.InstanceName = cc.InstanceName;
	client.ClientCallbackExecutorThreads = cc.ClientCallbackExecutorThreads;
	client.PollNameServerInterval = cc.PollNameServerInterval;
	client.HeartbeatBrokerInterval = cc.HeartbeatBrokerInterval;
	client.PersistConsumerOffsetInterval = cc.PersistConsumerOffsetInterval;
}



