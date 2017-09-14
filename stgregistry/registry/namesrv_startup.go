package registry

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/parseutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
	"os"
)

// Startup 设置默认启动的控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func Startup() *DefaultNamesrvController {
	controller := CreateNamesrvController()

	if initResult := controller.initialize(); !initResult {
		controller.shutdown()
		fmt.Println("controller initialize fail.")
		logger.Info("controller initialize fail.")
		os.Exit(-3)
	}

	//TODO:设置JVM关闭钩子，当JVM关闭之前，执行controller.shutdown()来关闭Namesrv服务控制，然后再关闭JVM
	controller.start()

	tip := "The Name Server boot success."
	logger.Info(tip)
	fmt.Println(tip)

	return controller
}

func CreateNamesrvController() *DefaultNamesrvController {
	logger.Info("createNamesrvController() start ... ")
	// 加载配置文件
	var namesrvConfig namesrv.DefaultNamesrvConfig
	cfgPath := "../../conf/smartgoKVConfig.toml"
	parseutil.ParseConf(cfgPath, &namesrvConfig)

	if namesrvConfig.SmartgoHome != "" {
		logger.Info("smartgoKVConfig.SmartgoHome=%s", namesrvConfig.SmartgoHome)
	}
	if namesrvConfig.KvConfigPath != "" {
		logger.Info("smartgoKVConfig.KvConfigPath=%s", namesrvConfig.KvConfigPath)
	}

	if namesrvConfig.GetSmartGoHome() == "" {
		cfg := namesrv.NewNamesrvConfig()
		namesrvConfig.KvConfigPath = cfg.GetKvConfigPath()
		namesrvConfig.SmartgoHome = cfg.GetSmartGoHome()
		if namesrvConfig.GetSmartGoHome() == "" {
			msg := "Please set the %s variable in your environment to match the location of the smartgo installation\n"
			fmt.Printf(msg, stgcommon.SMARTGO_HOME_ENV)
			os.Exit(-2)
		}
	}

	// 初始化NamesrvController
	remotingServer := remoting.NewDefalutRemotingServer("0.0.0.0", 9876)
	controller := NewNamesrvController(&namesrvConfig, remotingServer)

	logger.Info("createNamesrvController() end ... ")
	return controller
}
