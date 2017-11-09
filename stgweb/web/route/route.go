package route

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web"
	"git.oschina.net/cloudzone/smartgo/stgregistry/other"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/controller/broker"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/controller/cluster"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/controller/general"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/controller/group"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/controller/message"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/controller/topic"
	"github.com/kataras/iris/context"
)

func Route(ctx *web.Context) error {
	route := ctx.Route()
	api := route.Party("/api/v1/")
	{
		api.Options("/{root:path}", func(ctx context.Context) {}) // fix options not match bug
	}

	// 整体概览
	{
		api.Get("/general", general.GeneralStats)
	}

	// cluster集群管理
	{
		api.Get("/cluster/general", cluster.ClusterGeneral)
		api.Get("/cluster/list", cluster.ClusterList)
	}

	// topic管理
	{
		api.Post("/topic", topic.CreateTopic)
		api.Put("/topic", topic.UpdateTopic)
		api.Delete("/topic", topic.DeleteTopic)
		api.Get("/topic/list", topic.TopicList)
		api.Get("/topic/stats", topic.TopicStats)
		api.Get("/topic/route", topic.TopicRoute)
	}

	// 消费进度
	{
		api.Get("/group/progress", group.ConnectionDetail)
		api.Get("/group/list", group.ConsumeProgress)
	}

	// 消费进程
	{
		api.Get("/connection/online", group.ConnectionOnline)
		api.Get("/connection/detail", group.ConnectionDetail)
	}

	// 消息查询、消费轨迹
	{
		api.Get("/msg/body", message.MessageBody)
		api.Get("/msg/track", message.MessageQuery)
		api.Get("/msg/query", message.MessageQuery)
	}

	// 运维
	{
		api.Delete("/consumer/subGroup", broker.DeleteSubGroup)
		api.Put("/consumer/subGroup", broker.UpdateSubGroup)
		api.Post("/broker/syncTopic", broker.SyncTopicToBroker)
		api.Post("/broker/wipePerm", broker.WipeWritePermBroker)
	}

	return nil
}
