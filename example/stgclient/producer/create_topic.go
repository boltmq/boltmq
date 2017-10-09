package main
import (
	"time"

	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"encoding/json"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"
	"fmt"
)

func TaskSync() {
	t := time.NewTicker(time.Second * 1000)
	for {
		select {
		case <-t.C:
		}

	}
}

func main() {
	heartbeat:=&heartbeat.HeartbeatDataPlus{}
	data:=`{"clientID":"10.122.1.128@5676","producerDataSet":[{"groupName":"CLIENT_INNER_PRODUCER"}],"consumerDataSet":[{"groupName":"myConsumerGroup","consumeType":1,"messageModel":1,"consumeFromWhere":0,"subscriptionDataSet":[{"classFilterMode":false,"topic":"cloudzone1","subString":"tagA","tagsSet":["tagA"],"codeSet":[3552231],"subVersion":0},{"classFilterMode":false,"topic":"%RETRY%myConsumerGroup","subString":"*","tagsSet":[],"codeSet":[],"subVersion":0}],"unitMode":false}]}`
	json.Unmarshal([]byte(data),heartbeat)
	abc:=heartbeat.ConsumerDataSet[0].SubscriptionDataSet[0]
	fmt.Println(abc.Topic)
	defaultMQProducer := process.NewDefaultMQProducer("producer")
	defaultMQProducer.SetNamesrvAddr("10.122.2.28:9876")
	defaultMQProducer.Start()
	defaultMQProducer.CreateTopic(stgcommon.DEFAULT_TOPIC, "cloudzone1", 8)
	go TaskSync()
	time.Sleep(time.Second * 10)
	defaultMQProducer.Shutdown()
}