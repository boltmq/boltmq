package stgbroker

import (
	"flag"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/parseutil"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"os"
	"strings"
)

type SmartgoBrokerConfig struct {
	BrokerClusterName string
	BrokerName        string
	DeleteWhen        int
	FileReservedTime  int
	BrokerRole        string
	FlushDiskType     string
}

func Start() *BrokerController {
	controller := CreateBrokerController()
	controller.Start()
	tip := "The broker[" + controller.BrokerConfig.BrokerName + ", " + controller.GetBrokerAddr() + "] boot success."

	if "" != controller.BrokerConfig.NamesrvAddr {
		tip += " and name server is " + controller.BrokerConfig.NamesrvAddr
	}

	fmt.Println(tip)

	return controller
}

func CreateBrokerController() *BrokerController {
	var smartgoBrokerConfig SmartgoBrokerConfig

	// 加载配置文件
	brokerConfigPath := flag.String("c", "../../conf/smartgoBroker.toml", "")
	flag.Parse()
	parseutil.ParseConf(*brokerConfigPath, &smartgoBrokerConfig)

	// 初始化brokerConfig
	brokerConfig := stgcommon.NewBrokerConfig()

	brokerConfig.BrokerName = smartgoBrokerConfig.BrokerName
	brokerConfig.BrokerClusterName = smartgoBrokerConfig.BrokerClusterName

	// 如果没有设置home环境变量，则启动失败
	if "" == brokerConfig.SmartGoHome {
		fmt.Println("Please set the " + stgcommon.SMARTGO_HOME_ENV + " variable in your environment to match the location of the RocketMQ installation")
		os.Exit(-2)
	}

	// 检测Name Server地址设置是否正确 IP:PORT
	namesrvAddr := brokerConfig.NamesrvAddr
	if "" != namesrvAddr {
		addrArray := strings.Split(namesrvAddr, ";")
		if addrArray != nil {
			for _, value := range addrArray {
				ipAndport := strings.Split(value, ":")
				if ipAndport == nil {
					os.Exit(-3)
				}
			}
		} else {
			os.Exit(-3)
		}
	} else {
		os.Exit(-3)
	}

	// 初始化brokerConfig
	messageStoreConfig := stgstorelog.NewMessageStoreConfig()
	// 如果是slave，修改默认值（修改命中消息在内存的最大比例40为30【40-10】）
	if config.SLAVE == messageStoreConfig.BrokerRole {
		ratio := messageStoreConfig.AccessMessageInMemoryMaxRatio - 10
		messageStoreConfig.AccessMessageInMemoryMaxRatio = ratio
	}

	// BrokerId的处理（switch-case语法：只要匹配到一个case，则顺序往下执行，直到遇到break，因此若没有break则不管后续case匹配与否都会执行）
	switch messageStoreConfig.BrokerRole {
	//如果是同步master也会执行下述case中brokerConfig.setBrokerId(MixAll.MASTER_ID);语句，直到遇到break
	case config.ASYNC_MASTER:
	case config.SYNC_MASTER:
		brokerConfig.BrokerId = stgcommon.MASTER_ID
	case config.SLAVE:
		if brokerConfig.BrokerId <= 0 {
			fmt.Println("Slave's brokerId must be > 0")
			os.Exit(-3)
		}
	default:

	}

	// 初始化日志
	controller := NewBrokerController(*brokerConfig, messageStoreConfig)
	controller.ConfigFile = brokerConfigPath

	// 初始化controller
	initResult := controller.Initialize()
	if !initResult {
		controller.Shutdown()
		os.Exit(-3)
	}

	return controller
}
