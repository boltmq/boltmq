package consumer

// MQPushConsumer: push消费接口
// Author: yintongqiang
// Since:  2017/8/25

type MQPushConsumer interface {
	// 开启
	Start()
	// 关闭
	Shutdown()
}
