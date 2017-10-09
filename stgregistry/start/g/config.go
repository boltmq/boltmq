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
	cfgName = "cfg.json"
)

var (
	cfg Config
)

func configPath() string {
	scfg := os.Getenv("SMARTGO_CONFIG")
	if strings.TrimSpace(scfg) == "" {
		dirPath := stgcommon.GetSmartgoConfigDir(cfg)
		return dirPath + cfgName
	}
	return scfg
}

// Init 模块初始化
func InitLogger() {
	cfgPath := configPath()
	err := utils.ParseConfig(cfgPath, &cfg)
	if err != nil {
		panic(err)
	}
	log.Printf("read config file %s success.\n", cfgPath)
	logger.SetConfig(cfg.Log)
}
