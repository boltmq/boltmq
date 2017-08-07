package stgclient

import (
	"fmt"
	"testing"
)
/*
    Description: producer和consumer公共配置

    Author: yintongqiang
    Since:  2017/8/7
 */

func TestNewClientConfig(t *testing.T) {
	clientConfig := NewClientConfig("127.0.0.1:9876")
	fmt.Println(clientConfig.InstanceName)
	fmt.Println(clientConfig.ClientIP)
	clientConfig.ChangeInstanceNameToPID()
	fmt.Println(clientConfig.InstanceName)
	fmt.Println(clientConfig.BuildMQClientId())
}


