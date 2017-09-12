package mqtrace

// SendMessageHook 发送消息回调
// Author rongzhihong
// Since 2017/9/5
type SendMessageHook interface {
	HookName() string
	SendMessageBefore(context *SendMessageContext)
	SendMessageAfter(context *SendMessageContext)
}
