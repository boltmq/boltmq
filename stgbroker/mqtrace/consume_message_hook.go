package mqtrace

// ConsumeMessageHook 消费消息回调
// Author rongzhihong
// Since 2017/9/5
type ConsumeMessageHook interface {
	HookName() string
	ConsumeMessageBefore(context *ConsumeMessageContext)
	ConsumeMessageAfter(context *ConsumeMessageContext)
}
