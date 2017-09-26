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

	formatBroker := "the broker[%s, %s] boot success."
	tips := fmt.Sprintf(formatBroker, controller.BrokerConfig.BrokerName, controller.GetBrokerAddr())

	if "" != controller.BrokerConfig.NamesrvAddr {
		formatNamesrv := "the broker[%s, %s] boot success, and the name server is %s"
		tips = fmt.Sprintf(formatNamesrv, controller.BrokerConfig.BrokerName, controller.GetBrokerAddr(), controller.BrokerConfig.NamesrvAddr)
	}
	fmt.Println(tips)
	return controller
}

func CreateBrokerController() *BrokerController {
	var cfg SmartgoBrokerConfig

	// 加载配置文件
	brokerConfigPath := flag.String("c", "../../conf/smartgoBroker.toml", "")
	flag.Parse()
	parseutil.ParseConf(*brokerConfigPath, &cfg)

	// 初始化brokerConfig
	brokerConfig := stgcommon.NewBrokerConfig()

	brokerConfig.BrokerName = cfg.BrokerName
	brokerConfig.BrokerClusterName = cfg.BrokerClusterName

	// 如果没有设置home环境变量，则启动失败
	if "" == brokerConfig.SmartGoHome {
		errMsg := fmt.Sprintf("Please set the '%s' variable in your environment to match the location of the Smartgo installation\n", stgcommon.SMARTGO_HOME_ENV)
		fmt.Printf(errMsg)
		os.Exit(0)
	}

	// 检测环境变量NAMESRV_ADDR
	nameSrvAddr := brokerConfig.NamesrvAddr
	if strings.TrimSpace(nameSrvAddr) == "" {
		errMsg := fmt.Sprintf("Please set the '%s' variable in your environment\n", stgcommon.NAMESRV_ADDR_ENV)
		fmt.Printf(errMsg)
		os.Exit(0)
	}

	// 检测NameServer环境变量设置是否正确 IP:PORT
	addrs := strings.Split(strings.TrimSpace(nameSrvAddr), ";")
	if addrs == nil || len(addrs) == 0 {
		errMsg := fmt.Sprintf("the %s=%s environment variable is invalid. \n", stgcommon.NAMESRV_ADDR_ENV, addrs)
		fmt.Printf(errMsg)
		os.Exit(0)
	}
	for _, addr := range addrs {
		ipAndPort := strings.Split(addr, ":")
		if ipAndPort == nil {
			errMsg := fmt.Sprintf("the ipAndPort[%s] is invalid. \n", addr)
			fmt.Printf(errMsg)
			os.Exit(0)
		}
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
			fmt.Printf("Slave's brokerId[%d] must be > 0 \n", brokerConfig.BrokerId)
			os.Exit(0)
		}
	default:

	}

	// 初始化日志
	controller := NewBrokerController(*brokerConfig, messageStoreConfig)
	controller.ConfigFile = *brokerConfigPath

	// 初始化controller
	initResult := controller.Initialize()
	if !initResult {
		fmt.Println("the broker initialize failed")
		controller.Shutdown()
		os.Exit(0)
	}

	return controller
}
