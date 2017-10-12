package main

import (
	"fmt"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	namesrvAddr     = "10.112.68.189:9876"
	topic           = "cloudzone2"
	tag             = "tagA"
	producerGroupId = "producerGroupId-200"
	goThreadNum     = 100
	everyThreadNum  = 1000
)

func main() {
	var wg sync.WaitGroup
	defaultMQProducer := process.NewDefaultMQProducer(producerGroupId)
	defaultMQProducer.SetNamesrvAddr(namesrvAddr)
	defaultMQProducer.Start()
	var sucCount int64
	var failCount int64
	start := time.Now().Unix()
	for i := 0; i < goThreadNum; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < everyThreadNum; j++ {
				sendResult, err := defaultMQProducer.Send(message.NewMessage(topic, tag, []byte("I'm so diao!呵呵"+strconv.Itoa(n)+"-"+strconv.Itoa(j))))
				if err != nil {
					atomic.AddInt64(&failCount, 1)
					fmt.Println("send msg err: ----> ", err.Error())
					continue
				}
				if sendResult != nil {
					atomic.AddInt64(&sucCount, 1)
					fmt.Println(sendResult.ToString())
				}
			}
		}(i)
	}
	wg.Wait()
	end := time.Now().Unix()
	tps := sucCount / (end - start)
	fmt.Println("msgCount=", goThreadNum*everyThreadNum, "goThreadNum=", goThreadNum, "everyThreadNum=", everyThreadNum, "successCount=", sucCount, "failCount=", failCount, "tps=", tps)
}
