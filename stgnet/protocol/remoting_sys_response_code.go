package protocol

// RemotingSysResponseCode: xxx
// Author: yintongqiang
// Since:  2017/8/16

const (
	// 成功
	SUCCESS = 0
	// 发生了未捕获异常
	SYSTEM_ERROR = 1
	// 由于线程池拥堵，系统繁忙
	SYSTEM_BUSY = 2
	// 请求代码不支持
	REQUEST_CODE_NOT_SUPPORTED = 3
	//事务失败，添加db失败
	TRANSACTION_FAILED = 4
)
