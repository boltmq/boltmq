package test

import (
	"testing"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
)

func TestSubscriptionGroupManager(t *testing.T) {
	brokerController:=stgbroker.CreateBrokerController()
	subscriptionGroupManager:=stgbroker.NewSubscriptionGroupManager(brokerController)
	subscriptionGroupManager.Load()
	fmt.Println(subscriptionGroupManager.SubscriptionGroupTable.Size())
	subscriptionGroupManager.ConfigManagerExt.Persist()
}