package main

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgcommon/static"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"os"
	"os/signal"
	"syscall"
)

func buildMaster() *stgbroker.BrokerController {
	os.Setenv("NAMESRV_ADDR", "127.0.0.1:9876")
	brokerController := stgbroker.CreateBrokerController("E:\\goprj\\src\\git.oschina.net\\cloudzone\\smartgo\\broker-a.toml")
	brokerController.BrokerConfig.BrokerName = "broker-group"
	brokerController.BrokerConfig.BrokerId = 0
	brokerController.BrokerConfig.BrokerClusterName = "ha-cluster"
	brokerController.BrokerConfig.NamesrvAddr = "127.0.0.1:9876"
	brokerController.BrokerConfig.StorePathRootDir = "C:\\Users\\Administrator\\store-master"

	brokerController.MessageStoreConfig.StorePathRootDir = "C:\\Users\\Administrator\\store-master"
	brokerController.MessageStoreConfig.BrokerRole = config.SYNC_MASTER
	brokerController.MessageStoreConfig.HaListenPort = 10912
	brokerController.MessageStoreConfig.StorePathCommitLog = "C:\\Users\\Administrator\\store-master\\commitlog"
	brokerController.MessageStoreConfig.MapedFileSizeCommitLog = 1024 * 1024

	brokerController.RemotingServer = remoting.NewDefalutRemotingServer(static.BROKER_IP, 10911)
	brokerController.ConfigFile = "C:\\Users\\Administrator\\store-master"

	return brokerController
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	master := buildMaster()
	master.Initialize()
	master.Start()

	<-signalChan
	master.Shutdown()
	close(signalChan)
}
