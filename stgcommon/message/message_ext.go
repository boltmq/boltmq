package message
// MessageExt: 消息扩展
// Author: yintongqiang
// Since:  2017/8/10

type MessageExt struct {
	// 消息主题
	Topic                     string
	// 消息标志，系统不做干预，完全由应用决定如何使用
	Flag                      int
	// 消息属性，都是系统属性，禁止应用设置
	Properties                map[string]string
	// 消息体
	Body                      []byte
	// 队列ID <PUT>
	queueId                   int
	// 存储记录大小
	storeSize                 int
	// 队列偏移量
	queueOffset               int64
	// 消息标志位 <PUT>
	sysFlag                   int
	// 消息在客户端创建时间戳 <PUT>
	bornTimestamp             int64
	// 消息来自哪里 <PUT>
	bornHost                  string
	// 消息在服务器存储时间戳
	storeTimestamp            int64
	// 消息存储在哪个服务器 <PUT>
	storeHost                 string
	// 消息ID
	msgId                     string
	// 消息对应的Commit Log Offset
	commitLogOffset           int64
	// 消息体CRC
	bodyCRC                   int
	// 当前消息被某个订阅组重新消费了几次（订阅组之间独立计数）
	reconsumeTimes            int
	preparedTransactionOffset int64
}
