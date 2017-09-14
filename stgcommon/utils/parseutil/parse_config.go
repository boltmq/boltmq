package parseutil

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

// ParseConf 用于加载解析toml配置文件
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/7/29
func ParseConf(path string, configStruct interface{}) {

	if _, err := toml.DecodeFile(path, configStruct); err != nil {
		fmt.Println(err)
		return
	}
}
