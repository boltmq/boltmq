package main

import (
	"flag"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)

var (
	def_producerGroupId = "producerGroupId-200"
	def_topic           = "cloudzone123"
	def_namesrvAddr     = "127.0.0.1:9876"
	def_threadNum       = 3600
)

// 启动命令 ./topic -p producerGroupId-200 -topic cloudzoneA -h 10.112.68.189:9876
func main() {
	producerGroupId := flag.String("p", def_producerGroupId, "the producer group id ")
	topic := flag.String("topic", def_topic, "the topic for use")
	namesrvAddr := flag.String("h", def_namesrvAddr, "the namesrv ip:port")
	flag.Parse()

	defaultMQProducer := process.NewDefaultMQProducer(*producerGroupId)
	defaultMQProducer.SetNamesrvAddr(*namesrvAddr)
	defaultMQProducer.Start()
	defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, *topic, 8)
	defaultMQProducer.Shutdown()
}
