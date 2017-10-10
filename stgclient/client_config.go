package stgclient

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
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
	return &ClientConfig{NamesrvAddr: namesrvAddr,
		InstanceName:                  instanceName,
		ClientIP:                      GetLocalAddress(),
		ClientCallbackExecutorThreads: runtime.NumCPU(),
		PollNameServerInterval:        1000 * 30,
		HeartbeatBrokerInterval:       1000 * 30,
		PersistConsumerOffsetInterval: 1000 * 5}
}

func GetLocalAddress() string {
	adds, _ := net.InterfaceAddrs()
	for _, address := range adds {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil && !isIntranetIpv4(ipnet.IP.String()) {
				return ipnet.IP.String()
			}

		}
	}
	return ""
}

func isIntranetIpv4(ip string) bool {
	if strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "169.254.") {
		return true
	}
	return false
}

func (conf *ClientConfig) BuildMQClientId() string {
	unixNano := time.Now().UnixNano()
	hashCode := hashCode(conf.NamesrvAddr)
	return fmt.Sprintf("%s@%d#%d#%d", conf.ClientIP, os.Getpid(), hashCode, unixNano)
	//return fmt.Sprintf("%s@%s", conf.ClientIP, conf.InstanceName)
}

func (client *ClientConfig) ChangeInstanceNameToPID() {
	if strings.EqualFold(client.InstanceName, "DEFAULT") {
		client.InstanceName = strconv.Itoa(os.Getpid())
	}
}

func (client *ClientConfig) CloneClientConfig() *ClientConfig {
	return &ClientConfig{NamesrvAddr: client.NamesrvAddr,
		InstanceName:                  client.InstanceName,
		ClientIP:                      client.ClientIP,
		ClientCallbackExecutorThreads: client.ClientCallbackExecutorThreads,
		PollNameServerInterval:        client.PollNameServerInterval,
		HeartbeatBrokerInterval:       client.HeartbeatBrokerInterval,
		PersistConsumerOffsetInterval: client.PersistConsumerOffsetInterval}
}

func (client *ClientConfig) ResetClientConfig(cc *ClientConfig) {
	client.NamesrvAddr = cc.NamesrvAddr
	client.ClientIP = cc.ClientIP
	client.InstanceName = cc.InstanceName
	client.ClientCallbackExecutorThreads = cc.ClientCallbackExecutorThreads
	client.PollNameServerInterval = cc.PollNameServerInterval
	client.HeartbeatBrokerInterval = cc.HeartbeatBrokerInterval
	client.PersistConsumerOffsetInterval = cc.PersistConsumerOffsetInterval
}

func hashCode(s string) int64 {
	var h int64
	for i := 0; i < len(s); i++ {
		h = 31*h + int64(s[i])
	}
	return h
}
