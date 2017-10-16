package stgbroker

import (
	"fmt"
	"os"
	"strings"

	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/parseutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgstorelog"
	"git.oschina.net/cloudzone/smartgo/stgstorelog/config"
	"github.com/toolkits/file"
)

const (
	brokerConfigName = "smartgoBroker.toml"
)

// Start 启动BrokerController
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/20
func Start(stopChan chan bool, smartgoBrokerFileName string) *BrokerController {
	// 构建BrokerController控制器、初始化BrokerController
	controller := CreateBrokerController(smartgoBrokerFileName)

	// 注册ShutdownHook钩子
	controller.registerShutdownHook(stopChan)

	// 启动BrokerController
	controller.Start()

	format := "the broker[%s, %s] boot success."
	tips := fmt.Sprintf(format, controller.BrokerConfig.BrokerName, controller.GetBrokerAddr())
	if controller.BrokerConfig.NamesrvAddr != "" {
		format = "the broker[%s, %s] boot success, and the name server is %s"
		tips = fmt.Sprintf(format, controller.BrokerConfig.BrokerName, controller.GetBrokerAddr(), controller.BrokerConfig.NamesrvAddr)
	}
	fmt.Println(tips) // 此处不要使用logger.Info(),给nohup.out提示

	return controller
}

// CreateBrokerController 创建BrokerController对象
//
// 注意：
// (1)通过IDEA编辑器，启动test()用例、启动main()入口，两种方式读取conf/smartgoBroker.toml得到的相对路径有所区别
// (2)如果在服务器通过cmd命令行读取conf/smartgoBroker.toml，则可以正常读取
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/20
func CreateBrokerController(smartgoBrokerFileName ...string) *BrokerController {
	// 读取并转化*.toml配置项的值
	cfg, ok := ParseSmartgoBrokerConfig(smartgoBrokerFileName...)
	if !ok || cfg == nil {
		logger.Flush()
		os.Exit(0)
	}

	// 初始化brokerConfig，并校验broker启动的所必需的SmartGoHome、Namesrv配置
	brokerConfig := stgcommon.NewCustomBrokerConfig(cfg)
	logger.Infof("broker.UserHomeDir && store.StorePathRootDir = %s", brokerConfig.SmartgoDataPath)
	if !brokerConfig.CheckBrokerConfigAttr() {
		logger.Flush()
		os.Exit(0)
	}

	// 初始化brokerConfig、messageStoreConfig
	messageStoreConfig := stgstorelog.NewMessageStoreConfig()
	if !checkMessageStoreConfigAttr(messageStoreConfig, brokerConfig) {
		logger.Flush()
		os.Exit(0)
	}
	setMessageStoreConfig(messageStoreConfig, brokerConfig)

	// 构建BrokerController结构体
	remotingClient := remoting.NewDefalutRemotingClient()
	controller := NewBrokerController(brokerConfig, messageStoreConfig, remotingClient)
	controller.ConfigFile = brokerConfig.SmartgoDataPath

	// 初始化controller
	initResult := controller.Initialize()
	if !initResult {
		fmt.Println("the broker controller initialize failed")
		controller.Shutdown()
		logger.Flush()
		os.Exit(0)
	}

	logger.Info("create broker controller successful")
	return controller
}

// ParseSmartgoBrokerConfig 读取并转化Broker启动所必须的配置文件
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/22
func ParseSmartgoBrokerConfig(smartgoBrokerFileName ...string) (*stgcommon.SmartgoBrokerConfig, bool) {
	cfgName := getSmartgoBrokerConfigName(smartgoBrokerFileName...)
	cfgPath := stgcommon.GetSmartGoHome() + "/conf/" + cfgName // 各种main()启动broker,读取环境变量对应的路径
	if !file.IsExist(cfgPath) {
		firstPath := cfgPath
		firstPath = "../../conf/" + cfgName // 各种test用例启动broker,读取相对路径
		if !file.IsExist(firstPath) {
			cfgPath = stgcommon.GetSmartgoConfigDir() + cfgName // 在IDEA上面利用conf/smartgoBroker.toml默认配置文件目录
			logger.Infof("idea special brokerConfigPath = %s", cfgPath)
		}
	}

	// 读取并转化*.toml配置项的值
	var cfg stgcommon.SmartgoBrokerConfig
	parseutil.ParseConf(cfgPath, &cfg)
	if &cfg == nil {
		logger.Errorf("read %s failed", cfgPath)
		return nil, false
	}

	logger.Info(cfg.ToString())
	if cfg.IsBlank() {
		logger.Errorf("please set `brokerClusterName` and `brokerName` value with %s", cfgName)
		return nil, false
	}

	// TODO:处理broker、store等模块的存取数据目录, 优先级从高到低:
	// smartgoBroker.toml文件SmartgoDataPath属性值 >> 环境变量“SMARTGO_DATA_PATH” >> 操作系统的user.Current().HomeDir属性
	if strings.TrimSpace(cfg.SmartgoDataPath) == "" {
		cfg.SmartgoDataPath = stgcommon.GetUserHomeDir() + separator + "store"
	}
	return &cfg, true
}

// getSmartgoBrokerConfigName 获得启动broker的toml文件名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/10/16
func getSmartgoBrokerConfigName(smartgoBrokerFileName ...string) string {
	if smartgoBrokerFileName != nil && len(smartgoBrokerFileName) > 0 && smartgoBrokerFileName[0] != "" {
		return strings.TrimSpace(smartgoBrokerFileName[0])
	}
	return brokerConfigName
}

// checkMessageStoreConfigAttr 校验messageStoreConfig配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/22
func checkMessageStoreConfigAttr(mscfg *stgstorelog.MessageStoreConfig, bcfg *stgcommon.BrokerConfig) bool {
	if mscfg.BrokerRole == config.SLAVE && bcfg.BrokerId <= 0 {
		logger.Infof("Slave's brokerId[%d] must be > 0", bcfg.BrokerId)
		return false
	}
	return true
}

// setMessageStoreConfig 设置messageStoreConfig配置
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/22
func setMessageStoreConfig(messageStoreConfig *stgstorelog.MessageStoreConfig, brokerConfig *stgcommon.BrokerConfig) {
	// 此处需要覆盖store模块的StorePathRootDir配置目录,用来处理一台服务器启动多个broker的场景
	messageStoreConfig.StorePathRootDir = brokerConfig.SmartgoDataPath

	// 如果是slave，修改默认值（修改命中消息在内存的最大比例40为30【40-10】）
	if messageStoreConfig.BrokerRole == config.SLAVE {
		ratio := messageStoreConfig.AccessMessageInMemoryMaxRatio - 10
		messageStoreConfig.AccessMessageInMemoryMaxRatio = ratio
	}

	// BrokerId的处理 switch-case语法：
	// 只要匹配到一个case，则顺序往下执行，直到遇到break，因此若没有break则不管后续case匹配与否都会执行
	switch messageStoreConfig.BrokerRole {
	//如果是同步master也会执行下述case中brokerConfig.setBrokerId(MixAll.MASTER_ID);语句，直到遇到break
	case config.ASYNC_MASTER:
	case config.SYNC_MASTER:
		brokerConfig.BrokerId = stgcommon.MASTER_ID
	case config.SLAVE:
	default:

	}
}
