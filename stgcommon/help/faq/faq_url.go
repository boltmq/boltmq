package faq

import (
	"fmt"
	"strings"
)

const (
	APPLY_TOPIC_URL                = "https://github.com/alibaba/RocketMQ/issues/38" // FAQ: Topic不存在如何解决
	NAME_SERVER_ADDR_NOT_EXIST_URL = "https://github.com/alibaba/RocketMQ/issues/39" // FAQ: Name Server地址不存在
	GROUP_NAME_DUPLICATE_URL       = "https://github.com/alibaba/RocketMQ/issues/40" // FAQ: 启动Producer、Consumer失败，Group Name重复
	CLIENT_PARAMETER_CHECK_URL     = "https://github.com/alibaba/RocketMQ/issues/41" // FAQ: 客户端对象参数校验合法性
	SUBSCRIPTION_GROUP_NOT_EXIST   = "https://github.com/alibaba/RocketMQ/issues/42" // FAQ: 订阅组不存在如何解决
	CLIENT_SERVICE_NOT_OK          = "https://github.com/alibaba/RocketMQ/issues/43" // FAQ: producer、Consumer服务状态不正确
	NO_TOPIC_ROUTE_INFO            = "https://github.com/alibaba/RocketMQ/issues/44" // FAQ: No route info of this topic, TopicABC
	LOAD_JSON_EXCEPTION            = "https://github.com/alibaba/RocketMQ/issues/45" // FAQ: 广播消费者启动加载json文件异常问题
	SAME_GROUP_DIFFERENT_TOPIC     = "https://github.com/alibaba/RocketMQ/issues/46" // FAQ: 同一个订阅组内不同Consumer实例订阅关系不同
	MQLIST_NOT_EXIST               = "https://github.com/alibaba/RocketMQ/issues/47" // FAQ: 主动订阅消息，获取队列列表报Topic不存在
	UNEXPECTED_EXCEPTION_URL       = "https://github.com/alibaba/RocketMQ/issues/48" // FAQ: 未收录异常处理办法
	SEND_MSG_FAILED                = "https://github.com/alibaba/RocketMQ/issues/50" // FAQ: 发送消息尝试多次失败
	UNKNOWN_HOST_EXCEPTION         = "https://github.com/alibaba/RocketMQ/issues/64" // FAQ: 主机名不存在
	TipStringBegin                 = "\nSee "                                        // FAQ: 开始提示的内容
	TipStringEnd                   = " for further details."                         // FAQ: 结尾提示的内容
)

// SuggestTodo 建议从xxx获得帮助
func SuggestTodo(url string) string {
	value := fmt.Sprintf("%s%s%s", TipStringBegin, url, TipStringEnd)
	return value
}

// AttachDefaultURL 对于没有未异常原因指定FAQ的情况，追加默认FAQ
func AttachDefaultURL(errorMessage string) string {
	value := strings.TrimSpace(errorMessage)
	if value == "" {
		return value
	}

	index := strings.Index(errorMessage, TipStringBegin)
	if index < 0 {
		value = fmt.Sprintf("%s\nFor more information, please visit the url, %s", errorMessage, UNEXPECTED_EXCEPTION_URL)
	}

	return value
}
