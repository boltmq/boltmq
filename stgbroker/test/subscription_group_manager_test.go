package test

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon/subscription"
	"testing"
)

func TestSubscriptionGroupManagerLoad(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	subscriptionGroupManager := stgbroker.NewSubscriptionGroupManager(brokerController)
	subscriptionGroupManager.Load()
	fmt.Println(subscriptionGroupManager.SubscriptionGroupTable.Size())
}

func TestFindSubscriptionGroupConfig(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	subscriptionGroupManager := stgbroker.NewSubscriptionGroupManager(brokerController)
	subscriptionGroupManager.Load()
	subscriptionGroupManager.FindSubscriptionGroupConfig("testGroup")
	fmt.Println(subscriptionGroupManager.FindSubscriptionGroupConfig("testGroup").GroupName)
}

func TestConfigFilePath(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	subscriptionGroupManager := stgbroker.NewSubscriptionGroupManager(brokerController)
	fmt.Println(subscriptionGroupManager.ConfigFilePath())
}

func TestUpdateSubscriptionGroupConfig(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	subscriptionGroupManager := stgbroker.NewSubscriptionGroupManager(brokerController)
	subscriptionGroupManager.Load()
	config := &subscription.SubscriptionGroupConfig{
		GroupName:      "testGroup",
		BrokerId:       123,
		RetryQueueNums: 123,
	}
	subscriptionGroupManager.UpdateSubscriptionGroupConfig(config)
	fmt.Println(subscriptionGroupManager.FindSubscriptionGroupConfig("testGroup").GroupName)
}

func TestDeleteSubscriptionGroupConfig(t *testing.T) {
	brokerController := stgbroker.CreateBrokerController()
	subscriptionGroupManager:= stgbroker.NewSubscriptionGroupManager(brokerController)
	subscriptionGroupManager.Load()
	fmt.Println(subscriptionGroupManager.SubscriptionGroupTable.Size())
	subscriptionGroupManager.DeleteSubscriptionGroupConfig("testGroup")
	fmt.Println(subscriptionGroupManager.SubscriptionGroupTable.Size())
	//fmt.Println(subscriptionGroupManager.FindSubscriptionGroupConfig("testGroup").GroupName)
}
