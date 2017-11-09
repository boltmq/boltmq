package messageService

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/clusterService"
	"github.com/toolkits/file"
	"sync"
)

var (
	messageService *MessageService
	sOnce          sync.Once
)

const ()

// MessageService 消息Message管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type MessageService struct {
	*modules.AbstractService
	clusterServiceImpl *clusterService.BoltMQClusterService
}

// Default 返回默认唯一对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *MessageService {
	sOnce.Do(func() {
		messageService = NewMessageService()
		messageService.clusterServiceImpl = clusterService.NewBoltMQClusterService()
	})
	return topicService
}

// NewMessageService 初始化Message管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewMessageService() *MessageService {
	blotmqMessageService := &MessageService{
		AbstractService: modules.NewAbstractService(),
	}
	return blotmqMessageService
}

// QueryMsgBody 查询消息Body内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *MessageService) QueryMsgBody(msgId string) (*models.MessageBodyVo, error) {
	defer utils.RecoveredFn()
	msgBodyPath := stgcommon.MSG_BODY_DIR + msgId

	if file.IsExist(msgBodyPath) {
		msgBody, err := file.ToString(msgBodyPath)
		if err != nil {
			return nil, err
		}
		messageBodyVo := models.NewMessageBodyVo(msgId, msgBody)
		return messageBodyVo, nil
	}

	// 查询消息、得到body内容、写入文件
}

// writeMsgBody 将消息body内容写入指定的目录
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *MessageService) writeMsgBody(msgBodyPath, msgbody string) (int, error) {
	defer utils.RecoveredFn()
	return file.WriteString(msgBodyPath, msgbody)
}

// writeMsgBody 将消息body内容写入指定的目录
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *MessageService) readMsgBody(msgBodyPath string) (string, error) {
	defer utils.RecoveredFn()
	if !file.IsExist(msgBodyPath) {
		return "", fmt.Errorf("file[%s] not exist", msgBodyPath)
	}
	return file.ToString(msgBodyPath)
}

// QueryMsg 查询消息结果
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *MessageService) QueryMsg(msgId string) (*message.MessageExt, error) {
	defer utils.RecoveredFn()

	service.InitMQAdmin()
	service.Start()
	defer service.Shutdown()

	messageExt, err := service.DefaultMQAdminExtImpl.ViewMessage(msgId)
	if err != nil {
		return nil, err
	}
	return messageExt, nil
}
