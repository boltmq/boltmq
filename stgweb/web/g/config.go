package g

import (
	"git.oschina.net/cloudzone/cloudcommon-go/utils"
	"git.oschina.net/cloudzone/cloudcommon-go/web"
	"log"
	"os"
)

type Config struct {
	Web web.Config `json:"web" toml:"web"`
}

var (
	cfg Config
)

func configPath() string {
	mcfg := os.Getenv("BLOTMQ_WEB_CONFIG")
	if mcfg == "" {
		return "etc/cfg.json"
	}

	return mcfg
}

//Init 模块初始化
func Init() {
	err := utils.ParseConfig(configPath(), &cfg)
	if err != nil {
		panic(err)
	}
	log.Println("read config file success.")
}

// GetConfig 取配置
func GetConfig() *Config {
	return &cfg
}
