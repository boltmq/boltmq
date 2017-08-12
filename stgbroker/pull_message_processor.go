package stgbroker


// PullMessageProcessor 拉消息请求处理
// Author gaoyanlei
// Since 2017/8/10
type PullMessageProcessor struct {
	// TODO Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

	BrokerController *BrokerController
}


// NewPullMessageProcessor 初始化PullMessageProcessor
// Author gaoyanlei
// Since 2017/8/9
func NewPullMessageProcessor(brokerController *BrokerController) *PullMessageProcessor {
	var pullMessageProcessor = new(PullMessageProcessor)
	pullMessageProcessor.BrokerController = brokerController
	return pullMessageProcessor
}