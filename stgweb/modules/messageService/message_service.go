package messageService

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message/track"
	code "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/groupGervice"
	"github.com/toolkits/file"
	"sync"
)

var (
	messageServ *MessageService
	sOnce       sync.Once
)

const ()

// MessageService 消息Message管理器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
type MessageService struct {
	*modules.AbstractService
	GroupServ *groupGervice.GroupService
}

// Default 返回默认唯一对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func Default() *MessageService {
	sOnce.Do(func() {
		messageServ = NewMessageService()
	})
	return messageServ
}

// NewMessageService 初始化
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func NewMessageService() *MessageService {
	return &MessageService{
		AbstractService: modules.Default(),
		GroupServ:       groupGervice.Default(),
	}
}

// QueryMsgBody 查询消息Body内容
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *MessageService) QueryMsgBody(msgId string) (*models.MessageBodyVo, error) {
	defer utils.RecoveredFn()
	msgBodyPath := service.getMsgBodyPath(msgId)

	if file.IsExist(msgBodyPath) {
		msgBody, err := file.ToString(msgBodyPath)
		if err != nil {
			return nil, err
		}
		messageBodyVo := models.NewMessageBodyVo(msgId, msgBody)
		return messageBodyVo, nil
	}

	// 查询消息、得到body内容、写入文件
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()
	messageExt, err := defaultMQAdminExt.ViewMessage(msgId)
	if err != nil {
		return nil, err
	}

	msgBody := string(messageExt.Body)
	_, err = service.writeMsgBody(msgBodyPath, messageExt.Body)
	if err != nil {
		return nil, err
	}

	msgBodyVo := models.NewMessageBodyVo(msgId, msgBody)
	return msgBodyVo, nil
}

// writeMsgBody 将消息body内容写入指定的目录
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *MessageService) writeMsgBody(msgBodyPath string, msgbody []byte) (int, error) {
	defer utils.RecoveredFn()
	return file.WriteBytes(msgBodyPath, msgbody)
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
func (service *MessageService) QueryMsg(msgId string) (*models.BlotMessage, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	messageExt, err := defaultMQAdminExt.ViewMessage(msgId)
	if err != nil {
		return nil, err
	}

	msgBodyPath := stgcommon.MSG_BODY_DIR + msgId
	_, err = service.writeMsgBody(msgBodyPath, messageExt.Body)
	if err != nil {
		return nil, err
	}

	tracks, err := defaultMQAdminExt.MessageTrackDetail(messageExt)
	if err != nil {
		return nil, err
	}

	messageExtVo := models.ToMessageExtVo(messageExt)
	blotMessage := models.NewBlotMessage(messageExtVo, tracks, msgBodyPath)
	return blotMessage, nil
}

// QueryMsg 查询消息结果
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func (service *MessageService) MessageTrack(msgId string) (*models.MessageTrackExtWapper, error) {
	defer utils.RecoveredFn()
	defaultMQAdminExt := service.GetDefaultMQAdminExtImpl()
	defaultMQAdminExt.Start()
	defer defaultMQAdminExt.Shutdown()

	result := models.NewMessageTrackExtWapper(msgId)
	messageExt, err := defaultMQAdminExt.ViewMessage(msgId)
	if err != nil {
		return result, err
	}

	result.OriginMsgId = messageExt.GetOriginMessageID()

	bronIp, _ := stgcommon.ParseClientAddr(messageExt.BornHost)
	produceTrackVo := &models.ProduceTrackVo{
		BornIp:            bronIp,
		BornTimestamp:     stgcommon.FormatTimestamp(messageExt.BornTimestamp),
		ProducerGroupId:   "",
		SendTimeConsuming: (messageExt.StoreTimestamp - messageExt.BornTimestamp),
	}
	result.TrackWapper.ProduceExt = produceTrackVo

	topicTrackVo := &models.TopicTrackVo{
		Topic: messageExt.Topic,
		Key:   messageExt.GetKeys(),
		Tag:   messageExt.GetTags(),
	}
	result.TrackWapper.TopicExt = topicTrackVo

	groupIds, err := service.GroupServ.QueryConsumerGroupId(messageExt.Topic)
	if err != nil {
		return result, err
	}
	if groupIds == nil || len(groupIds) == 0 {
		result.TrackWapper.ConsumeExt = make([]*models.ConsumeTrackVo, 0)
		return result, nil
	}

	consumeExts := make([]*models.ConsumeTrackVo, 0)
	for _, consumerGroupId := range groupIds {
		consumeExt := &models.ConsumeTrackVo{}
		consumerConnection, err := defaultMQAdminExt.ExamineConsumerConnectionInfo(consumerGroupId, messageExt.Topic)
		if err != nil {
			logger.Errorf("query consumerConnection err: %s.  consumerGroupId=%s", err.Error(), consumerGroupId)
		} else {
			connectionVos := make([]*models.ConnectionVo, 0)
			if consumerConnection != nil && consumerConnection.ConnectionSet != nil {
				for itor := range consumerConnection.ConnectionSet.Iterator().C {
					if connection, ok := itor.(*body.Connection); ok {
						ip, pid := stgcommon.ParseClientAddr(connection.ClientAddr)
						connectionVo := &models.ConnectionVo{IP: ip, PID: int64(pid)}
						connectionVos = append(connectionVos, connectionVo)
					}
				}
			}
			consumeExt.Connection = connectionVos

			tracks, err := defaultMQAdminExt.MessageTrackDetail(messageExt)
			if err != nil {
				return result, nil
			}
			if tracks != nil && len(tracks) > 0 {
				for _, msgTrack := range tracks {
					if msgTrack.ConsumerGroupId == consumerGroupId {
						consumeExt.ConsumerGroupId = msgTrack.ConsumerGroupId
						if msgTrack.TrackType == track.SubscribedAndConsumed {
							consumeExt.Code = code.SUCCESS
						} else {
							consumeExt.Code = code.SYSTEM_ERROR
						}
					}
				}
			}

			consumeExts = append(consumeExts, consumeExt)
		}
	}
	result.TrackWapper.ConsumeExt = consumeExts

	return result, nil

}

// getMsgBodyPath 获取消息内容的拓展存储路径
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/13
func (service *MessageService) getMsgBodyPath(msgId string) string {
	msgBodyPath := stgcommon.MSG_BODY_DIR + msgId
	return msgBodyPath
}
