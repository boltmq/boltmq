package protocol

// RemotingSysResponseCode: xxx
// Author: yintongqiang
// Since:  2017/8/16
const (
	SUCCESS                    = 0 // 成功
	SYSTEM_ERROR               = 1 // 发生了未捕获异常
	SYSTEM_BUSY                = 2 // 由于线程池拥堵，系统繁忙
	REQUEST_CODE_NOT_SUPPORTED = 3 // 请求代码不支持
	TRANSACTION_FAILED         = 4 // 事务失败，添加db失败
)
