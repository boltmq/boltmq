# 日志

### 级别
* Trace(format string, args ...interface{}) 打印trace级别日志
* Debug(format string, args ...interface{}) Debug 打印debug级别日志
* Info(format string, args ...interface{}) 打印info级别日志
* Warn(format string, args ...interface{}) 打印warn级别日志
* Error(format string, args ...interface{}) 打印error级别日志
* Fatal(format string, args ...interface{}) 打印critial级别日志

### 配置

* 日志配置方法 
```go
func Config(conf LogConfig)
```
注意： **不调用配置方法，日志默认打印到终端。**

* 配置日志打印到终端
```go
{
    "log": {
        "engine": {
            "adapter": "console",
            "config": {
            }
        },
        "path": "",
        "cache_size": 10000,
        "enable_func_call_depth": true,
        "func_call_depth": 3
    }
}
```

* 配置日志打印到文件
```go
{
    "log": {
        "engine": {
            "adapter": "file",
            "config": {
                "filename": "test.log"
            }
        },
        "path": "",
        "cache_size": 10000,
        "enable_func_call_depth": true,
        "func_call_depth": 3
    }
}
```

注意： **按天生成日志文件。**

### 代码示例
```go
package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"

	"git.oschina.net/cloudzone/smartgocommon/logger"

)

// Config test example
type Config struct {
	LogConfig logger.LogConfig `json:"log"`
}

func main() {

	logger.Trace("this is a test")
	logger.Debug("this is a test")
	logger.Info("this is a test")
	logger.Warn("this is a test")
	logger.Error("this is a test")
	logger.Fatal("this is a test")

	c := ParseConfig("cfg_file.json")
	logger.Config(c.LogConfig)

	logger.Trace("this is a test")
	logger.Debug("this is a test")
	logger.Info("this is a test")
	logger.Warn("this is a test")
	logger.Error("this is a test")
	logger.Fatal("this is a test")
}

// ParseConfig parse config from cfg file
func ParseConfig(cfg string) *Config {

	configContent, err := ToTrimString(cfg)
	if err != nil {
		log.Fatalln("read config file:", cfg, "fail:", err)
	}

	var c Config
	err = json.Unmarshal([]byte(configContent), &c)
	if err != nil {
		log.Fatalln("parse config file:", cfg, "fail:", err)
	}

	log.Println("read config file:", cfg, "successfully.", c)
	return &c
}

// ToString return string from read cfg file
func ToString(filePath string) (string, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ToTrimString read cfg file and replace space
func ToTrimString(filePath string) (string, error) {
	str, err := ToString(filePath)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(str), nil
}
```
