package process

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgclient/consumer"
	"time"
)

// PullMessageService: 拉取服务
// Author: yintongqiang
// Since:  2017/8/8

type PullMessageService struct {
	MQClientFactory  *MQClientInstance
	PullRequestQueue chan *consumer.PullRequest
	isStopped         bool
}

func NewPullMessageService(mqClientFactory *MQClientInstance) *PullMessageService {
	return &PullMessageService{MQClientFactory:mqClientFactory, PullRequestQueue:make(chan *consumer.PullRequest)}
}

func (service *PullMessageService) Start() {
	go func() {
		service.run()
	}()
}
func (service *PullMessageService) Shutdown() {
	service.isStopped = true
}

// 向通道中加入pullRequest
func (service *PullMessageService) ExecutePullRequestImmediately(pullRequest *consumer.PullRequest) {
	service.PullRequestQueue <- pullRequest
}

// 延迟执行pull请求
func (service *PullMessageService) ExecutePullRequestLater(pullRequest *consumer.PullRequest, timeDelay int) {
	go func() {
		time.Sleep(time.Millisecond * time.Duration(timeDelay))
		service.ExecutePullRequestImmediately(pullRequest)
	}()
}

func (service *PullMessageService) run() {
	logger.Infof("service started")
	for !service.isStopped {
		request := <-service.PullRequestQueue
		service.pullMessage(request)

	}
}

func (service *PullMessageService) pullMessage(pullRequest *consumer.PullRequest) {
	mConsumer := service.MQClientFactory.selectConsumer(pullRequest.ConsumerGroup)
	if mConsumer != nil {
		mConsumer.(*DefaultMQPushConsumerImpl).pullMessage(pullRequest)
	}
}