package consumer

// MQPullConsumer: xxx
// Author: yintongqiang
// Since:  2017/8/10

type MQPullConsumer interface {
	// 开启
	Start()
    // 关闭
  	Shutdown()
}
