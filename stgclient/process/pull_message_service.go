package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
)

// PullMessageService: 拉取服务
// Author: yintongqiang
// Since:  2017/8/8

type PullMessageService struct {
	MQClientFactory  *MQClientInstance
	PullRequestQueue chan consumer.PullRequest
}

func NewPullMessageService(mqClientFactory *MQClientInstance) *PullMessageService {
	return &PullMessageService{MQClientFactory:mqClientFactory, PullRequestQueue:make(chan consumer.PullRequest)}
}

func (service *PullMessageService) Start() {
	service.run()
}

func (service *PullMessageService) run() {
	logger.Info(" service started")
	for {
		request:=<-service.PullRequestQueue
		service.pullMessage(request)

	}
}

func (service *PullMessageService) pullMessage(pullRequest consumer.PullRequest) {
	service.MQClientFactory.selectConsumer(pullRequest.ConsumerGroup)
}