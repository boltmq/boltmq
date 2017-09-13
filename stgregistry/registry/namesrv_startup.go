package registry

import (
	"flag"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/parseutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"os"
)

type NamesrvStartup struct {
}

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
	logger.Info("tip")
	fmt.Println(tip)

	return controller
}

func CreateNamesrvController() *DefaultNamesrvController {
	// 加载配置文件
	var namesrvCfg *namesrv.DefaultNamesrvConfig
	kvConfigPath := flag.String("c", namesrv.GetKvConfigPath(), "")
	flag.Parse()
	parseutil.ParseConf(*kvConfigPath, &namesrvCfg)
	if namesrvCfg.GetSmartGoHome() == "" {
		msg := "Please set the %s variable in your environment to match the location of the smartgo installation\n"
		fmt.Printf(msg, stgcommon.SMARTGO_HOME_ENV)
		os.Exit(-2)
	}

	// 初始化NamesrvController
	remotingServer := remoting.NewDefalutRemotingServer("0.0.0.0", 9876)
	controller := NewNamesrvController(namesrvCfg, remotingServer)

	return controller
}
