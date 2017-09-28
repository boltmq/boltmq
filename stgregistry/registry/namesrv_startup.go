package registry

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
	"os"
)

const (
	port = 9876
)

// Startup 启动Namesrv控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func Startup() *DefaultNamesrvController {
	controller := CreateNamesrvController()
	initResult := controller.initialize()
	if !initResult {
		logger.Info("the name server controller initialize failed")
		controller.shutdown()
		os.Exit(0)
	}
	controller.start()
	logger.Info("the name server boot success")
	return controller
}

// CreateNamesrvController 创建默认Namesrv控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/15
func CreateNamesrvController() *DefaultNamesrvController {
	// 初始化配置文件
	cfg := namesrv.NewNamesrvConfig()
	logger.Info("%s", cfg.ToString())

	if cfg.GetSmartGoHome() == "" {
		msg := "Please set the %s variable in your environment to match the location of the smartgo installation\n"
		logger.Error(msg, stgcommon.SMARTGO_HOME_ENV)
		os.Exit(0)
	}

	// 初始化NamesrvController
	remotingServer := remoting.NewDefalutRemotingServer("0.0.0.0", port)
	controller := NewNamesrvController(cfg, remotingServer)

	logger.Info("create name server controller success")
	return controller
}
