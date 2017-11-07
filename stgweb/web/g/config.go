package g

import (
	"git.oschina.net/cloudzone/cloudcommon-go/utils"
	"git.oschina.net/cloudzone/cloudcommon-go/web"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"github.com/toolkits/file"
	"log"
	"os"
)

type Config struct {
	Web web.Config `json:"web" toml:"web"`
}

const (
	cfgName = "cfg.json"
)

var (
	cfg Config
)

func configPath() string {
	cfgPath := os.Getenv(stgcommon.BLOTMQ_WEB_CONFIG_ENV)
	if file.IsExist(cfgPath) {
		return cfgPath
	}

	// 默认寻找当前目录的cfg.json日志配置文件
	cfgPath = file.SelfDir() + string(os.PathSeparator) + "etc" + string(os.PathSeparator) + cfgName
	if file.IsExist(cfgPath) {
		return cfgPath
	}

	return "etc/cfg.json"
}

//Init 模块初始化
func Init() {
	cfgPath := configPath()
	err := utils.ParseConfig(cfgPath, &cfg)
	if err != nil {
		panic(err)
	}
	log.Println("read config file success.")
}

// GetConfig 取配置
func GetConfig() *Config {
	return &cfg
}
