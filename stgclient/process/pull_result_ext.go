package process

import "git.oschina.net/cloudzone/smartgo/stgclient/consumer"

type PullResultExt struct {
	*consumer.PullResult
	suggestWhichBrokerId int
	messageBinary        [] byte
}
