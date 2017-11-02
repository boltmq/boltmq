package modules

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"os"
)

type ConfigureInitializer struct {
	NamesrvAddr string `toml:"namesrvAddr"`
}

func NewConfigureInitializer() *ConfigureInitializer {
	configureInitializer := new(ConfigureInitializer)
	configureInitializer.NamesrvAddr = os.Getenv(stgcommon.NAMESRV_ADDR_ENV)
	return configureInitializer
}
