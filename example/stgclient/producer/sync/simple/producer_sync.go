package main

import (
	"flag"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strconv"
	"strings"
)

var (
	def_producerGroupId = "producerGroupId-200"
	def_topic           = "cloudzone123"
	def_tag             = "tagA"
	def_namesrvAddr     = "10.112.68.189:9876"
	def_bodyDataSize    = 0
	def_threadNum       = 3600
)

func main() {

	producerGroupId := flag.String("p", def_producerGroupId, "the producer group id ")
	topic := flag.String("topic", def_topic, "the topic for use")
	tag := flag.String("tag", def_tag, "the tag for use")
	namesrvAddr := flag.String("h", def_namesrvAddr, "the namesrv ip:port")
	gonum := flag.Int("n", def_threadNum, "go thread num")
	bodyDataSize := flag.Int("s", def_bodyDataSize, "send data size")

	flag.Parse()

	defaultMQProducer := process.NewDefaultMQProducer(*producerGroupId)
	defaultMQProducer.SetNamesrvAddr(*namesrvAddr)
	defaultMQProducer.Start()
	for i := 0; i < *gonum; i++ {
		body := "I'm so diao!呵呵" + strconv.Itoa(i)
		if *msgDataSize > 0 {
			body = strings.Repeat("a", *msgDataSize)
		}
		sendResult, err := defaultMQProducer.Send(message.NewMessage(*topic, *tag, []byte(body)))
		if err != nil {
			fmt.Println("send msg err: ----> ", err.Error())
			continue
		}
		if sendResult != nil {
			fmt.Println(sendResult.ToString())
		} else {
			fmt.Println("send msg failed: sendResult is nil")
		}
	}
	defaultMQProducer.Shutdown()
}
