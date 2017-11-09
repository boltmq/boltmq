package stgstorelog

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strconv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

// MessageExtBrokerInner 存储内部使用的Message对象
// Author gaoyanlei
// Since 2017/8/16
type MessageExtBrokerInner struct {
	message.MessageExt
	PropertiesString string
	TagsCode         int64
}

func (self *MessageExtBrokerInner) isWaitStoreMsgOK() bool {
	properties, ok := self.MessageExt.Message.Properties[message.PROPERTY_WAIT_STORE_MSG_OK]
	if !ok {
		return true
	}

	result, err := strconv.ParseBool(properties)
	if err != nil {
		logger.Warn("message parse wait store msg properties error, ", err.Error())
		return true
	}

	return result
}

func TagsString2tagsCode(filterType stgcommon.TopicFilterType, tags string) int64 {
	if tags == "" || len(tags) == 0 {
		return 0
	}
	return HashCode(tags)
}

func HashCode(s string) int64 {
	var h int64
	for i := 0; i < len(s); i++ {
		h = 31*h + int64(s[i])
	}
	return h
}
