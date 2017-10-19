package main

import (
	"flag"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	def_producerGroupId = "producerGroupId-200" // 生产者组ID
	def_topic           = "cloudzone123"        // topic
	def_tag             = "tagA"                // tags
	def_namesrvAddr     = "10.112.68.189:9876"  // namesrv地址
	def_threadNum       = 200                   // 线程个数
	def_everyThreadNum  = 50000                 // 每个线程发送消息次数
	def_bodyDataSize    = 20                    // body数据长度
)

// 启动命令(1) ./tps -p producerGroupId-200 -topic cloudzone123 -tag tagA -h 10.112.68.189:9876 -n 200 -c 50000 -s 20
// 启动命令(2)  ./tps -n 200 -c 50000 -s 20
func main() {

	producerGroupId := flag.String("p", def_producerGroupId, "the producer group id ")
	topic := flag.String("topic", def_topic, "the topic for use")
	tag := flag.String("tag", def_tag, "the tag for use")
	namesrvAddr := flag.String("h", def_namesrvAddr, "the namesrv ip:port")
	gonum := flag.Int("n", def_threadNum, "go thread num")
	sendnum := flag.Int("c", def_everyThreadNum, "one thread send msg count")
	bodyDataSize := flag.Int("s", def_bodyDataSize, "send data size")

	flag.Parse()

	var (
		wg             sync.WaitGroup
		goThreadNum    = *gonum
		everyThreadNum = *sendnum
		sucCount       int64
		failCount      int64
		start          int64
		end            int64
		tps            int64
	)
	defaultMQProducer := process.NewDefaultMQProducer(*producerGroupId)
	defaultMQProducer.SetNamesrvAddr(*namesrvAddr)
	defaultMQProducer.Start()

	start = time.Now().Unix()
	for i := 0; i < goThreadNum; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < everyThreadNum; j++ {
				// body := "I'm so diao!呵呵" + strconv.Itoa(n) + "-" + strconv.Itoa(j)
				body := strings.Repeat("a", *bodyDataSize)
				sendResult, err := defaultMQProducer.Send(message.NewMessage(*topic, *tag, []byte(body)))
				if err != nil || sendResult == nil {
					atomic.AddInt64(&failCount, 1)
					if err != nil {
						fmt.Println("send msg err: ----> ", err.Error())
					}
					continue
				}
				atomic.AddInt64(&sucCount, 1)
				//fmt.Println(sendResult.ToString())
			}
		}(i)
	}
	wg.Wait()
	end = time.Now().Unix()
	tps = sucCount / (end - start)

	format := "msgCount=%d, goThreadNum=%d, everyThreadNum=%d, successCount=%d, failCount=%d, tps=%d \n"
	fmt.Printf(format, goThreadNum*everyThreadNum, goThreadNum, everyThreadNum, sucCount, failCount, tps)
}
