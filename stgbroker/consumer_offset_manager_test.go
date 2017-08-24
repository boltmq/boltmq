package stgbroker

import (
	"testing"
	"fmt"
)

func TestConsumerOffsetManager(t *testing.T) {
	brokerController := CreateBrokerController()
	a := NewConsumerOffsetManager(brokerController)
	a.Load()
	fmt.Println(a.Offsets.size())
	//fmt.Println(a.SubscriptionGroupConfigs[1].GroupName)

	//b := make(table[int]int64)
	//b[0] = int64(123)
	//
	//d:=newOffsetTable()
	//d.put("%RETRY%S_fundmng_demo_producer@S_fundmng_demo_producer",b)
	//a.Offsets = d
	a.configManagerExt.Persist()
}
