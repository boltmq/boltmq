package registry

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
	"os"
)

const (
	default_port = 9876
	default_ip   = "0.0.0.0" // 不能设置为127.0.0.1,否则别的集群无法访问当前机器的namesrv服务
)

// Startup 启动Namesrv控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/14
func Startup(stopChannel chan bool) *DefaultNamesrvController {
	// 构建NamesrvController
	controller := CreateNamesrvController()

	// NamesrvController初始化
	initResult := controller.initialize()
	if !initResult {
		fmt.Println("the name server controller initialize failed")
		controller.shutdown()
		os.Exit(0)
	}

	// 注册ShutdownHook钩子
	controller.registerShutdownHook(stopChannel)

	// 启动
	go func() {
		// 额外处理“RemotingServer.Stacr()启动后，导致channel缓冲区满，进而引发namesrv主线程阻塞”情况
		controller.startNamesrvController()
	}()
	fmt.Println("the name server boot success") // 此处不要使用logger.Info(),给nohup.out提示

	return controller
}

// CreateNamesrvController 创建默认Namesrv控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/15
func CreateNamesrvController() *DefaultNamesrvController {
	// 初始化配置文件
	cfg := namesrv.NewNamesrvConfig()
	logger.Info("%s", cfg.ToString())

	// 检查“SMARTGO_HOME”环境变量的值是为了，设置日志组件，main()已经初始化了logger,因此没必要额外判断
	//if cfg.GetSmartGoHome() == "" {
	//	msg := "Please set the %s variable in your environment to match the location of the smartgo installation\n"
	//	logger.Error(msg, stgcommon.SMARTGO_HOME_ENV)
	//	os.Exit(0)
	//}

	// 初始化NamesrvController
	remotingServer := remoting.NewDefalutRemotingServer(default_ip, default_port)
	controller := NewNamesrvController(cfg, remotingServer)

	logger.Info("create name server controller success")
	return controller
}
