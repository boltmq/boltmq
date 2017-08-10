package main

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/producer"
	"fmt"
	"time"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
)
// 实现枚举例子

type State int

const (
	SEND_OK State = iota
	FLUSH_DISK_TIMEOUT
	FLUSH_SLAVE_TIMEOUT
	SLAVE_NOT_AVAILABLE
)

func (state State) String() string {
	switch state {
	case SEND_OK:
		return "SEND_OK"
	case FLUSH_DISK_TIMEOUT:
		return "FLUSH_DISK_TIMEOUT"
	case FLUSH_SLAVE_TIMEOUT:
		return "FLUSH_SLAVE_TIMEOUT"
	case SLAVE_NOT_AVAILABLE:
		return "SLAVE_NOT_AVAILABLE"
	default:
		return "Unknow"
	}
}
func task1(t *time.Ticker)  {
	for {
		select {
		case <-t.C:
			fmt.Println("task1")
			t.Stop()
		}
	}
}
func task2(t *time.Ticker)  {
	for {
		select {
		case <-t.C:
			fmt.Println("task2")
		}
	}
}
func task3(t *time.Ticker)  {
	for {
		select {
		case <-t.C:
			fmt.Println("task3")
		}
	}
}
type ArrayTest struct {
	Names []string
	A ABC
	B *ABC
}
type ABC struct {
 B int
}
func main() {
	src:=[]byte("say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!say hello!")
	fmt.Println(len(src))
	cSrc:=stgcommon.Compress(src)
	fmt.Println(len(cSrc))
	fmt.Println(len(stgcommon.UnCompress(cSrc)))
	fmt.Println(string(stgcommon.UnCompress(cSrc)))
	//abc:=&ArrayTest{}
	//fmt.Print(abc.A)
	//fmt.Print(abc.B)
	//index:=1
	//a:=float64(index)
	//fmt.Print(a)
	//test:=&ArrayTest{}
	//for i:=0;i<100;i++{
	//	test.Names=append(test.Names,"1")
	//}
	defaultMQProducer := producer.NewDefaultMQProducer("producer")
	defaultMQProducer.SetNamesrvAddr("127.0.0.1:9876")
	defaultMQProducer.Start()
	defaultMQProducer.Send(message.NewMessage("TestTopic","tagA",[]byte("I'm so diao!")))
	defaultMQProducer.Send(message.NewMessage("TestTopic","tagA",[]byte("I'm so diao!")))

	//t.Stop()
	//time.Sleep(time.Second * 3)

	//select {
	//
	//}
}

