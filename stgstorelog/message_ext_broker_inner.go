package stgstorelog

import "git.oschina.net/cloudzone/smartgo/stgcommon/message"

type MessageExtBrokerInner struct {
	// TODO serialVersionUID
	message.MessageExt
	propertiesString string
	tagsCode         int64
}
