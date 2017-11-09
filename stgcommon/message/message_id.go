package message

type MessageId struct {
	Address string // 消息落地存储，角色为storeHost对应的brokerAddr
	Offset  uint64 // 消息落地存储，物理偏移量， 即 physicOffset、commitLogOffset
}
