package process

// SendCallback: 发送回调函数
// Author: yintongqiang
// Since:  2017/8/9

//type SendCallback interface {
//	OnSuccess(sendResult *SendResult)
//}
type SendCallback func(sendResult *SendResult, err error)
