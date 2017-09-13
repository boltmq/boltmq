package stgregistry

import (
	"flag"
	"git.oschina.net/cloudzone/smartgo/stgcommon/namesrv"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/parseutil"
)

type NamesrvStartup struct {
}

func Start() {

}

func CreateNamesrvController() *DefaultNamesrvController {

	// 加载配置文件
	var namesrvCfg namesrv.DefaultNamesrvConfig
	kvConfigPath := flag.String("c", namesrv.GetKvConfigPath(), "")
	flag.Parse()
	parseutil.ParseConf(*kvConfigPath, &namesrvCfg)

	// 初始化NamesrvController

	return nil
}
