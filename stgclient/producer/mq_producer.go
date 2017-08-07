package producer

/*
    Description: 客户对外使用的producer接口

    Author: yintongqiang
    Since:  2017/8/7
 */
import "git.oschina.net/cloudzone/smartgo/stgcommon/message"

type MQProducer interface {
	// 启动
	Start() error
	// 关闭
	Shutdown()
	// 同步发送消息
	Send(msg message.Message) (*SendResult, error)
}
