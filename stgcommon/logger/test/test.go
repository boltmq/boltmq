package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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
