package route

import (
	"git.oschina.net/cloudzone/cloudcommon-go/web"
	"git.oschina.net/cloudzone/smartgo/stgweb/web/controller/topic"
	"github.com/kataras/iris/context"
)

func Route(ctx *web.Context) error {
	route := ctx.Route()
	api := route.Party("/api/v1/")
	{
		api.Options("/{root:path}", func(ctx context.Context) {}) // fix options not match bug
	}

	// 概览
	{
		//api.Get("/general", general.General)
	}

	// cluster集群管理
	{

	}

	// topic管理
	{
		api.Post("/topic", topic.CreateTopic)
		api.Get("/topic/list", topic.TopicList)
	}

	// 消费进度
	{

	}

	// 消息查询
	{

	}

	// 运维
	{

	}

	return nil
}
