package protocol

import (
	"fmt"
)

// ResponseCode: 响应码
// Author: yintongqiang
// Since:  2017/8/16
const (
	SUCCESS                       = 0   // 成功
	SYSTEM_ERROR                  = 1   // 发生了未捕获异常
	SYSTEM_BUSY                   = 2   // 由于线程池拥堵，系统繁忙
	REQUEST_CODE_NOT_SUPPORTED    = 3   // 请求代码不支持
	TRANSACTION_FAILED            = 4   // 事务失败，添加db失败
	FLUSH_DISK_TIMEOUT            = 10  // Broker 刷盘超时
	SLAVE_NOT_AVAILABLE           = 11  // Broker 同步双写，Slave不可用
	FLUSH_SLAVE_TIMEOUT           = 12  // Broker 同步双写，等待Slave应答超时
	MESSAGE_ILLEGAL               = 13  // Broker 消息非法
	SERVICE_NOT_AVAILABLE         = 14  // Broker, Namesrv 服务不可用，可能是正在关闭或者权限问题
	VERSION_NOT_SUPPORTED         = 15  // Broker, Namesrv 版本号不支持
	NO_PERMISSION                 = 16  // Broker, Namesrv 无权限执行此操作，可能是发、收、或者其他操作
	TOPIC_NOT_EXIST               = 17  // Broker, Topic不存在
	TOPIC_EXIST_ALREADY           = 18  // Broker, Topic已经存在，创建Topic
	PULL_NOT_FOUND                = 19  // Broker 拉消息未找到（请求的Offset等于最大Offset，最大Offset无对应消息）
	PULL_RETRY_IMMEDIATELY        = 20  // Broker 可能被过滤，或者误通知等
	PULL_OFFSET_MOVED             = 21  // Broker 拉消息请求的Offset不合法，太小或太大
	QUERY_NOT_FOUND               = 22  // Broker 查询消息未找到
	SUBSCRIPTION_PARSE_FAILED     = 23  // Broker 订阅关系解析失败
	SUBSCRIPTION_NOT_EXIST        = 24  // Broker 订阅关系不存在
	SUBSCRIPTION_NOT_LATEST       = 25  // Broker 订阅关系不是最新的
	SUBSCRIPTION_GROUP_NOT_EXIST  = 26  // Broker 订阅组不存在
	TRANSACTION_SHOULD_COMMIT     = 200 // producer 事务应该被提交
	TRANSACTION_SHOULD_ROLLBACK   = 201 // producer 事务应该被回滚
	TRANSACTION_STATE_UNKNOW      = 202 // producer 事务状态未知
	TRANSACTION_STATE_GROUP_WRONG = 203 // producer ProducerGroup错误
	NO_BUYER_ID                   = 204 // 单元化消息，需要设置 buyerId
	NOT_IN_CURRENT_UNIT           = 205 // 单元化消息，非本单元消息
	CONSUMER_NOT_ONLINE           = 206 // Consumer不在线
	CONSUME_MSG_TIMEOUT           = 207 // Consumer消费消息超时
)

func ParseResponse(responseCode int32) string {
	value, ok := responsePattern[responseCode]
	if !ok {
		return fmt.Sprintf("unknown responseCode[%d]", responseCode)
	}
	return value
}

var responsePattern = map[int32]string{
	0:   "SUCCESS",
	1:   "SYSTEM_ERROR",
	2:   "SYSTEM_BUSY",
	3:   "REQUEST_CODE_NOT_SUPPORTED",
	4:   "TRANSACTION_FAILED",
	10:  "FLUSH_DISK_TIMEOUT",
	11:  "SLAVE_NOT_AVAILABLE",
	12:  "FLUSH_SLAVE_TIMEOUT",
	13:  "MESSAGE_ILLEGAL",
	14:  "SERVICE_NOT_AVAILABLE",
	15:  "VERSION_NOT_SUPPORTED",
	16:  "NO_PERMISSION",
	17:  "TOPIC_NOT_EXIST",
	18:  "TOPIC_EXIST_ALREADY",
	19:  "PULL_NOT_FOUND",
	20:  "PULL_RETRY_IMMEDIATELY",
	21:  "PULL_OFFSET_MOVED",
	22:  "QUERY_NOT_FOUND",
	23:  "SUBSCRIPTION_PARSE_FAILED",
	24:  "SUBSCRIPTION_NOT_EXIST",
	25:  "SUBSCRIPTION_NOT_LATEST",
	26:  "SUBSCRIPTION_GROUP_NOT_EXIST",
	200: "TRANSACTION_SHOULD_COMMIT",
	201: "TRANSACTION_SHOULD_ROLLBACK",
	202: "TRANSACTION_STATE_UNKNOW",
	203: "TRANSACTION_STATE_GROUP_WRONG",
	204: "NO_BUYER_ID",
	205: "NOT_IN_CURRENT_UNIT",
	206: "CONSUMER_NOT_ONLINE",
	207: "CONSUME_MSG_TIMEOUT",
}
