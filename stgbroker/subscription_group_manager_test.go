package stgbroker

import (
	"testing"
	"fmt"
)

func TestSubscriptionGroupManager(t *testing.T) {
	brokerController:=CreateBrokerController()
	a:=NewSubscriptionGroupManager(brokerController)
	a.Load()
	//fmt.Println(a.SubscriptionGroupConfigs[1].GroupName)
	fmt.Println(a.SubscriptionGroupTable.Size())

	a.configManagerExt.Persist()
}