package topic

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/cloudcommon-go/web/req"
	"git.oschina.net/cloudzone/cloudcommon-go/web/resp"
	"git.oschina.net/cloudzone/smartgo/stgweb/models"
	"git.oschina.net/cloudzone/smartgo/stgweb/modules/topicService"
	"github.com/kataras/iris/context"
	"strconv"
	"strings"
)

// TopicList 查询所有Topic列表
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func TopicList(ctx context.Context) {
	clusterName := strings.TrimSpace(ctx.URLParam("clusterName"))
	topic := strings.TrimSpace(ctx.URLParam("topic"))

	pageRequest, err := req.ToPageRequest(ctx)
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}
	limit := pageRequest.Limit
	offset := pageRequest.Offset

	extra, err := strconv.ParseBool(ctx.URLParam("extra"))
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}

	topicType, err := ctx.URLParamInt("topicType")
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}

	topicListVo, total, err := topicService.Default().GetTopicList(clusterName, topic, extra, topicType, limit, offset)
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	ctx.JSON(resp.NewSuccessPageResponse(total, topicListVo))
}

// TopicStats 查询Topic存储状态
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func TopicStats(ctx context.Context) {
	topic := strings.TrimSpace(ctx.URLParam("topic"))
	if topic == "" {
		errMsg := "topic字段不能为空"
		logger.Errorf("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, errMsg))
		return
	}

	pageRequest, err := req.ToPageRequest(ctx)
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}
	limit := pageRequest.Limit
	offset := pageRequest.Offset

	topicState, total, err := topicService.Default().GetTopicStats(topic, limit, offset)
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	ctx.JSON(resp.NewSuccessPageResponse(total, topicState))
}

// CreateTopic 创建Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/7
func CreateTopic(ctx context.Context) {
	topicVo := new(models.CreateTopic)
	if err := ctx.ReadJSON(topicVo); err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	if err := topicVo.Validate(); err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}

	topicVo.Topic = strings.TrimSpace(topicVo.Topic)
	topicVo.ClusterName = strings.TrimSpace(topicVo.ClusterName)

	err := topicService.Default().CreateTopic(topicVo)
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	responseBody := &models.ResultVo{Result: true}
	ctx.JSON(resp.NewSuccessResponse(responseBody))
}

// UpdateTopic 更新Topic配置信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func UpdateTopic(ctx context.Context) {
	topicVo := new(models.UpdateTopic)
	if err := ctx.ReadJSON(topicVo); err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	if err := topicVo.Validate(); err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}

	topicVo.Topic = strings.TrimSpace(topicVo.Topic)
	topicVo.ClusterName = strings.TrimSpace(topicVo.ClusterName)
	topicVo.BrokerAddr = strings.TrimSpace(topicVo.BrokerAddr)

	err := topicService.Default().UpdateTopicConfig(topicVo)
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	responseBody := &models.ResultVo{Result: true}
	ctx.JSON(resp.NewSuccessResponse(responseBody))
}

// DeleteTopic 删除Topic
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func DeleteTopic(ctx context.Context) {
	topicVo := new(models.DeleteTopic)
	if err := ctx.ReadJSON(topicVo); err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}
	if err := topicVo.Validate(); err != nil {
		logger.Warn("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ParamNotValid, err.Error()))
		return
	}

	err := topicService.Default().DeleteTopic(strings.TrimSpace(topicVo.Topic), strings.TrimSpace(topicVo.ClusterName))
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	responseBody := &models.ResultVo{Result: true}
	ctx.JSON(resp.NewSuccessResponse(responseBody))
}

// TopicRoute 查询Topic路由信息
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/9
func TopicRoute(ctx context.Context) {
	clusterName := strings.TrimSpace(ctx.URLParam("clusterName"))
	topic := strings.TrimSpace(ctx.URLParam("topic"))
	if topic == "" {
		errMsg := "topic字段不能为空"
		logger.Errorf("%s %s %s", errMsg, ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, errMsg))
		return
	}

	data, err := topicService.Default().QueryTopicRoute(topic, clusterName)
	if err != nil {
		logger.Errorf("%s %s %s", err.Error(), ctx.Method(), ctx.Path())
		ctx.JSON(resp.NewFailedResponse(resp.ResponseCodes.ServerError, err.Error()))
		return
	}

	ctx.JSON(resp.NewSuccessResponse(data))
}
