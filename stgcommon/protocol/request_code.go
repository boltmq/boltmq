package protocol

import (
	"fmt"
)

// RequestCode: 内部传输协议码
// Author: yintongqiang
// Since:  2017/8/10
const (
	SEND_MESSAGE                         = 10  // Broker 发送消息
	PULL_MESSAGE                         = 11  // Broker 订阅消息
	QUERY_MESSAGE                        = 12  // Broker 查询消息
	QUERY_BROKER_OFFSET                  = 13  // Broker 查询Broker Offset
	QUERY_CONSUMER_OFFSET                = 14  // Broker 查询Consumer Offset
	UPDATE_CONSUMER_OFFSET               = 15  // Broker 更新Consumer Offset
	UPDATE_AND_CREATE_TOPIC              = 17  // Broker 更新或者增加一个Topic
	GET_ALL_TOPIC_CONFIG                 = 21  // Broker 获取所有Topic的配置（Slave和Namesrv都会向Master请求此配置）
	GET_TOPIC_CONFIG_LIST                = 22  // Broker 获取所有Topic配置（Slave和Namesrv都会向Master请求此配置）
	GET_TOPIC_NAME_LIST                  = 23  // Broker 获取所有Topic名称列表
	UPDATE_BROKER_CONFIG                 = 25  // Broker 更新Broker上的配置
	GET_BROKER_CONFIG                    = 26  // Broker 获取Broker上的配置
	TRIGGER_DELETE_FILES                 = 27  // Broker 触发Broker删除文件
	GET_BROKER_RUNTIME_INFO              = 28  // Broker 获取Broker运行时信息
	SEARCH_OFFSET_BY_TIMESTAMP           = 29  // Broker 根据时间查询队列的Offset
	GET_MAX_OFFSET                       = 30  // Broker 查询队列最大Offset
	GET_MIN_OFFSET                       = 31  // Broker 查询队列最小Offset
	GET_EARLIEST_MSG_STORETIME           = 32  // Broker 查询队列最早消息对应时间
	VIEW_MESSAGE_BY_ID                   = 33  // Broker 根据消息ID来查询消息
	HEART_BEAT                           = 34  // Broker Client向Client发送心跳，并注册自身
	UNREGISTER_CLIENT                    = 35  // Broker Client注销
	CONSUMER_SEND_MSG_BACK               = 36  // Broker Consumer将处理不了的消息发回服务器
	END_TRANSACTION                      = 37  // Broker Commit或者Rollback事务
	GET_CONSUMER_LIST_BY_GROUP           = 38  // Broker 获取ConsumerId列表通过GroupName
	CHECK_TRANSACTION_STATE              = 39  // Broker 主动向Producer回查事务状态
	NOTIFY_CONSUMER_IDS_CHANGED          = 40  // Broker Broker通知Consumer列表变化
	LOCK_BATCH_MQ                        = 41  // Broker Consumer向Master锁定队列
	UNLOCK_BATCH_MQ                      = 42  // Broker Consumer向Master解锁队列
	GET_ALL_CONSUMER_OFFSET              = 43  // Broker 获取所有Consumer Offset
	GET_ALL_DELAY_OFFSET                 = 45  // Broker 获取所有定时进度
	PUT_KV_CONFIG                        = 100 // Namesrv 向Namesrv追加KV配置
	GET_KV_CONFIG                        = 101 // Namesrv 从Namesrv获取KV配置
	DELETE_KV_CONFIG                     = 102 // Namesrv 从Namesrv获取KV配置
	REGISTER_BROKER                      = 103 // Namesrv 注册一个Broker，数据都是持久化的，如果存在则覆盖配置
	UNREGISTER_BROKER                    = 104 // Namesrv 卸载一个Broker，数据都是持久化的
	GET_ROUTEINTO_BY_TOPIC               = 105 // Namesrv 根据Topic获取Broker Name、队列数(包含读队列与写队列)
	GET_BROKER_CLUSTER_INFO              = 106 // Namesrv 获取注册到Name Server的所有Broker集群信息
	UPDATE_AND_CREATE_SUBSCRIPTIONGROUP  = 200 // 创建或更新订阅组
	GET_ALL_SUBSCRIPTIONGROUP_CONFIG     = 201 // 订阅组配置
	GET_TOPIC_STATS_INFO                 = 202 // 统计信息，获取Topic统计信息
	GET_CONSUMER_CONNECTION_LIST         = 203 // Consumer连接管理
	GET_PRODUCER_CONNECTION_LIST         = 204 // Producer连接管理
	WIPE_WRITE_PERM_OF_BROKER            = 205 // 优雅地向Broker写数据
	GET_ALL_TOPIC_LIST_FROM_NAMESERVER   = 206 // 从Name Server获取完整Topic列表
	DELETE_SUBSCRIPTIONGROUP             = 207 // 从Broker删除订阅组
	GET_CONSUME_STATS                    = 208 // 从Broker获取消费状态（进度）
	SUSPEND_CONSUMER                     = 209 // Suspend Consumer消费过程
	RESUME_CONSUMER                      = 210 // Resume Consumer消费过程
	RESET_CONSUMER_OFFSET_IN_CONSUMER    = 211 // 重置Consumer Offset
	RESET_CONSUMER_OFFSET_IN_BROKER      = 212 // 重置Consumer Offset
	ADJUST_CONSUMER_THREAD_POOL          = 213 // 调整Consumer线程池数量
	WHO_CONSUME_THE_MESSAGE              = 214 // 查询消息被哪些消费组消费
	DELETE_TOPIC_IN_BROKER               = 215 // 从Broker删除Topic配置
	DELETE_TOPIC_IN_NAMESRV              = 216 // 从Namesrv删除Topic配置
	GET_KV_CONFIG_BY_VALUE               = 217 // Namesrv 通过 project 获取所有的 server ip 信息
	DELETE_KV_CONFIG_BY_VALUE            = 218 // Namesrv 删除指定 project group 下的所有 server ip 信息
	GET_KVLIST_BY_NAMESPACE              = 219 // 通过NameSpace获取所有的KV List
	RESET_CONSUMER_CLIENT_OFFSET         = 220 // offset 重置
	GET_CONSUMER_STATUS_FROM_CLIENT      = 221 // 客户端订阅消息
	INVOKE_BROKER_TO_RESET_OFFSET        = 222 // 通知 broker 调用 offset 重置处理
	INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223 // 通知 broker 调用客户端订阅消息处理
	QUERY_TOPIC_CONSUME_BY_WHO           = 300 // Broker 查询topic被谁消费 2014-03-21 Add By shijia
	GET_TOPICS_BY_CLUSTER                = 224 // 获取指定集群下的所有 topic 2014-03-26
	REGISTER_FILTER_SERVER               = 301 // 向Broker注册Filter Server 2014-04-06 Add By shijia
	REGISTER_MESSAGE_FILTER_CLASS        = 302 // 向Filter Server注册Class 2014-04-06 Add By shijia
	QUERY_CONSUME_TIME_SPAN              = 303 // 根据 topic 和 group 获取消息的时间跨度
	GET_SYSTEM_TOPIC_LIST_FROM_NS        = 304 // 从Namesrv获取所有系统内置 Topic 列表
	GET_SYSTEM_TOPIC_LIST_FROM_BROKER    = 305 // 从Broker获取所有系统内置 Topic 列表
	CLEAN_EXPIRED_CONSUMEQUEUE           = 306 // 清理失效队列
	GET_CONSUMER_RUNNING_INFO            = 307 // 通过Broker查询Consumer内存数据 2014-07-19 Add By shijia
	QUERY_CORRECTION_OFFSET              = 308 // 查找被修正 offset (转发组件）
	CONSUME_MESSAGE_DIRECTLY             = 309 // 通过Broker直接向某个Consumer发送一条消息，并立刻消费，返回结果给broker，再返回给调用方
	SEND_MESSAGE_V2                      = 310 // Broker 发送消息，优化网络数据包
	GET_UNIT_TOPIC_LIST                  = 311 // 单元化相关 topic
	GET_HAS_UNIT_SUB_TOPIC_LIST          = 312 // 获取含有单元化订阅组的 Topic 列表
	GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST   = 313 // 获取含有单元化订阅组的非单元化 Topic 列表
	CLONE_GROUP_OFFSET                   = 314 // 克隆某一个组的消费进度到新的组
	VIEW_BROKER_STATS_DATA               = 315 // 查看Broker上的各种统计信息
)

func ParseRequest(requestCode int32) string {
	value, ok := requestPattern[requestCode]
	if !ok {
		return fmt.Sprintf("unknown requestCode[%d]", requestCode)
	}
	return value
}

var requestPattern = map[int32]string{
	10:  "SEND_MESSAGE",
	11:  "PULL_MESSAGE",
	12:  "QUERY_MESSAGE",
	13:  "QUERY_BROKER_OFFSET",
	14:  "QUERY_CONSUMER_OFFSET",
	15:  "UPDATE_CONSUMER_OFFSET",
	17:  "UPDATE_AND_CREATE_TOPIC",
	21:  "GET_ALL_TOPIC_CONFIG",
	22:  "GET_TOPIC_CONFIG_LIST",
	23:  "GET_TOPIC_NAME_LIST",
	25:  "UPDATE_BROKER_CONFIG",
	26:  "GET_BROKER_CONFIG",
	27:  "TRIGGER_DELETE_FILES",
	28:  "GET_BROKER_RUNTIME_INFO",
	29:  "SEARCH_OFFSET_BY_TIMESTAMP",
	30:  "GET_MAX_OFFSET",
	31:  "GET_MIN_OFFSET",
	32:  "GET_EARLIEST_MSG_STORETIME",
	33:  "VIEW_MESSAGE_BY_ID",
	34:  "HEART_BEAT",
	35:  "UNREGISTER_CLIENT",
	36:  "CONSUMER_SEND_MSG_BACK",
	37:  "END_TRANSACTION",
	38:  "GET_CONSUMER_LIST_BY_GROUP",
	39:  "CHECK_TRANSACTION_STATE",
	40:  "NOTIFY_CONSUMER_IDS_CHANGED",
	41:  "LOCK_BATCH_MQ",
	42:  "UNLOCK_BATCH_MQ",
	43:  "GET_ALL_CONSUMER_OFFSET",
	45:  "GET_ALL_DELAY_OFFSET",
	100: "PUT_KV_CONFIG",
	101: "GET_KV_CONFIG",
	102: "DELETE_KV_CONFIG",
	103: "REGISTER_BROKER",
	104: "UNREGISTER_BROKER",
	105: "GET_ROUTEINTO_BY_TOPIC",
	106: "GET_BROKER_CLUSTER_INFO",
	200: "UPDATE_AND_CREATE_SUBSCRIPTIONGROUP",
	201: "GET_ALL_SUBSCRIPTIONGROUP_CONFIG",
	202: "GET_TOPIC_STATS_INFO",
	203: "GET_CONSUMER_CONNECTION_LIST",
	204: "GET_PRODUCER_CONNECTION_LIST",
	205: "WIPE_WRITE_PERM_OF_BROKER",
	206: "GET_ALL_TOPIC_LIST_FROM_NAMESERVER",
	207: "DELETE_SUBSCRIPTIONGROUP",
	208: "GET_CONSUME_STATS",
	209: "SUSPEND_CONSUMER",
	210: "RESUME_CONSUMER",
	211: "RESET_CONSUMER_OFFSET_IN_CONSUMER",
	212: "RESET_CONSUMER_OFFSET_IN_BROKER",
	213: "ADJUST_CONSUMER_THREAD_POOL",
	214: "WHO_CONSUME_THE_MESSAGE",
	215: "DELETE_TOPIC_IN_BROKER",
	216: "DELETE_TOPIC_IN_NAMESRV",
	217: "GET_KV_CONFIG_BY_VALUE",
	218: "DELETE_KV_CONFIG_BY_VALUE",
	219: "GET_KVLIST_BY_NAMESPACE",
	220: "RESET_CONSUMER_CLIENT_OFFSET",
	221: "GET_CONSUMER_STATUS_FROM_CLIENT",
	222: "INVOKE_BROKER_TO_RESET_OFFSET",
	223: "INVOKE_BROKER_TO_GET_CONSUMER_STATUS",
	300: "QUERY_TOPIC_CONSUME_BY_WHO",
	224: "GET_TOPICS_BY_CLUSTER",
	301: "REGISTER_FILTER_SERVER",
	302: "REGISTER_MESSAGE_FILTER_CLASS",
	303: "QUERY_CONSUME_TIME_SPAN",
	304: "GET_SYSTEM_TOPIC_LIST_FROM_NS",
	305: "GET_SYSTEM_TOPIC_LIST_FROM_BROKER",
	306: "CLEAN_EXPIRED_CONSUMEQUEUE",
	307: "GET_CONSUMER_RUNNING_INFO",
	308: "QUERY_CORRECTION_OFFSET",
	309: "CONSUME_MESSAGE_DIRECTLY",
	310: "SEND_MESSAGE_V2",
	311: "GET_UNIT_TOPIC_LIST",
	312: "GET_HAS_UNIT_SUB_TOPIC_LIST",
	313: "GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST",
	314: "CLONE_GROUP_OFFSET",
	315: "VIEW_BROKER_STATS_DATA",
}
