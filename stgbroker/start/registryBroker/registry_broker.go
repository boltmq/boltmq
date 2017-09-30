package main

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/header/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
)

func main() {
	namesrvAddr := "0.0.0.0:9876"
	oneway := false
	filter := []string{}

	brokerController := stgbroker.CreateBrokerController()
	brokerController.TopicConfigManager.Load()
	topicConfigWrapper := brokerController.TopicConfigManager.TopicConfigSerializeWrapper
	api := brokerController.BrokerOuterAPI
	api.Start()

	registerBrokerRequestList := initRegisterBrokerRequestHeader()
	for _, r := range registerBrokerRequestList {
		result := api.RegisterBroker(namesrvAddr, r.ClusterName, r.BrokerAddr, r.BrokerName, r.HaServerAddr, r.BrokerId, topicConfigWrapper, oneway, filter)
		logger.Info("result--> %s", result.ToString())
	}

	select {}
}

func initRegisterBrokerRequestHeader() []*namesrv.RegisterBrokerRequestHeader {
	clusterName := "DefaultCluster"
	registerBrokerRequestList := make([]*namesrv.RegisterBrokerRequestHeader, 0, 8)

	// broker-a
	masterA := &namesrv.RegisterBrokerRequestHeader{
		ClusterName:  clusterName,
		BrokerName:   "broker-name-a",
		BrokerAddr:   "192.168.1.100:10911",
		HaServerAddr: "192.168.1.100:10912",
		BrokerId:     int64(0),
	}
	registerBrokerRequestList = append(registerBrokerRequestList, masterA)

	slaveA := &namesrv.RegisterBrokerRequestHeader{
		ClusterName:  clusterName,
		BrokerName:   "broker-name-a",
		BrokerAddr:   "192.168.1.110:10911",
		HaServerAddr: "192.168.1.110:10912",
		BrokerId:     int64(1),
	}
	registerBrokerRequestList = append(registerBrokerRequestList, slaveA)

	// broker-b
	masterB := &namesrv.RegisterBrokerRequestHeader{
		ClusterName:  clusterName,
		BrokerName:   "broker-name-b",
		BrokerAddr:   "192.168.1.102:10911",
		HaServerAddr: "192.168.1.102:10912",
		BrokerId:     int64(0),
	}
	registerBrokerRequestList = append(registerBrokerRequestList, masterB)

	slaveB := &namesrv.RegisterBrokerRequestHeader{
		ClusterName:  clusterName,
		BrokerName:   "broker-name-b",
		BrokerAddr:   "192.168.1.112:10911",
		HaServerAddr: "192.168.1.112:10912",
		BrokerId:     int64(1),
	}
	registerBrokerRequestList = append(registerBrokerRequestList, slaveB)

	// broker-c
	masterC := &namesrv.RegisterBrokerRequestHeader{
		ClusterName:  clusterName,
		BrokerName:   "broker-name-c",
		BrokerAddr:   "192.168.1.104:10911",
		HaServerAddr: "192.168.1.104:10912",
		BrokerId:     int64(0),
	}
	registerBrokerRequestList = append(registerBrokerRequestList, masterC)

	slaveC := &namesrv.RegisterBrokerRequestHeader{
		ClusterName:  clusterName,
		BrokerName:   "broker-name-c",
		BrokerAddr:   "192.168.1.114:10911",
		HaServerAddr: "192.168.1.114:10912",
		BrokerId:     int64(1),
	}
	registerBrokerRequestList = append(registerBrokerRequestList, slaveC)

	// broker-d
	masterD := &namesrv.RegisterBrokerRequestHeader{
		ClusterName:  clusterName,
		BrokerName:   "broker-name-d",
		BrokerAddr:   "192.168.1.106:10911",
		HaServerAddr: "192.168.1.106:10912",
		BrokerId:     int64(0),
	}
	registerBrokerRequestList = append(registerBrokerRequestList, masterD)

	slaveD := &namesrv.RegisterBrokerRequestHeader{
		ClusterName:  clusterName,
		BrokerName:   "broker-name-d",
		BrokerAddr:   "192.168.1.116:10911",
		HaServerAddr: "192.168.1.116:10912",
		BrokerId:     int64(1),
	}
	registerBrokerRequestList = append(registerBrokerRequestList, slaveD)

	return registerBrokerRequestList
}
