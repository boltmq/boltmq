package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/producer"
)
// 实现枚举例子

type State int

const (
	SEND_OK State = iota
	FLUSH_DISK_TIMEOUT
	FLUSH_SLAVE_TIMEOUT
	SLAVE_NOT_AVAILABLE
)

func (state State) String() string {
	switch state {
	case SEND_OK:
		return "SEND_OK"
	case FLUSH_DISK_TIMEOUT:
		return "FLUSH_DISK_TIMEOUT"
	case FLUSH_SLAVE_TIMEOUT:
		return "FLUSH_SLAVE_TIMEOUT"
	case SLAVE_NOT_AVAILABLE:
		return "SLAVE_NOT_AVAILABLE"
	default:
		return "Unknow"
	}
}

func main() {
	defaultMQProducer := producer.NewDefaultMQProducer("producer")
	defaultMQProducer.SetNamesrvAddr("127.0.0.1:9876")
	defaultMQProducer.Start()
}
