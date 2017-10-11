package g

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgregistry/logger"
	"log"
	"os"
	"strings"
)

type Config struct {
	Log logger.Config `json:"log"`
}

const (
	cfgName             = "cfg.json"
	cacheSize           = 10000
	loggerFuncCallDepth = 3
	enableFuncCallDepth = false
	maxdays             = 30
	level               = 6
	filename            = "./logs/registry.log"
)

var (
	cfg Config
)

func configPath() string {
	cfgPath := os.Getenv("SMARTGO_REGISTRY_CONFIG")
	if strings.TrimSpace(cfgPath) == "" {
		dirPath := stgcommon.GetSmartgoConfigDir(cfg)
		return dirPath + cfgName
	}
	return cfgPath
}

// Init 模块初始化
func InitLogger() {
	cfgPath := configPath()
	err := utils.ParseConfig(cfgPath, &cfg)
	if err == nil {
		log.Printf("read config file %s success.\n", cfgPath)
		logger.SetConfig(cfg.Log)
	} else {
		log.Printf("set default config to logger")
		logger.SetConfig(getDefaultLoggerConfig())
	}
}

func getDefaultLoggerConfig() logger.Config {
	config := logger.Config{}
	config.CacheSize = cacheSize
	config.EnableFuncCallDepth = enableFuncCallDepth
	config.FuncCallDepth = loggerFuncCallDepth

	param := make(map[string]interface{})
	//param["filename"] = filename                      // 保存日志文件地址
	param["level"] = level                             // 日志级别 6:info, 7:debug
	param["maxdays"] = maxdays                         // 每天一个日志文件，最多保留文件个数,
	param["enableFuncCallDepth"] = enableFuncCallDepth // 打印日志，是否显示文件名
	param["loggerFuncCallDepth"] = loggerFuncCallDepth // 堆栈日志打印层数
	config.Engine.Config = param                       // 转化过去
	config.Engine.Adapter = loggerType.Console         // 日志类型 文件file、 控制台console

	return config
}

var loggerType = struct {
	File    string // 保存文件
	Console string // 打印到控制台
}{
	"file",
	"console",
}
