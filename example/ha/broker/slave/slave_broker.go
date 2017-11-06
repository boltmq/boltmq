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

func buildSlave(masterAddress string) *stgbroker.BrokerController {
	os.Setenv("NAMESRV_ADDR", "127.0.0.1:9876")

	brokerController := stgbroker.CreateBrokerController("E:\\goprj\\src\\git.oschina.net\\cloudzone\\smartgo\\broker-a.toml")
	brokerController.BrokerConfig.BrokerName = "broker-group"
	brokerController.BrokerConfig.BrokerId = 1
	brokerController.BrokerConfig.BrokerClusterName = "ha-cluster"
	brokerController.BrokerConfig.NamesrvAddr = "127.0.0.1:9876"
	brokerController.BrokerConfig.StorePathRootDir = "C:\\Users\\Administrator\\store-slave"

	brokerController.MessageStoreConfig.StorePathRootDir = "C:\\Users\\Administrator\\store-slave"
	brokerController.MessageStoreConfig.BrokerRole = config.SLAVE
	brokerController.MessageStoreConfig.HaListenPort = 10914
	brokerController.MessageStoreConfig.StorePathCommitLog = "C:\\Users\\Administrator\\store-slave\\commitlog"
	brokerController.MessageStoreConfig.MapedFileSizeCommitLog = 1024 * 1024
	brokerController.MessageStoreConfig.HaMasterAddress = "127.0.0.1:10912"

	brokerController.RemotingServer = remoting.NewDefalutRemotingServer(static.BROKER_IP, 10913)
	brokerController.ConfigFile = "C:\\Users\\Administrator\\store-slave"

	return brokerController
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	slave := buildSlave("127.0.0.1:10912")
	slave.Initialize()
	slave.Start()

	<-signalChan
	slave.Shutdown()
	close(signalChan)
}
