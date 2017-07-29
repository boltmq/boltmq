package logger

import (
	"encoding/json"

	"github.com/astaxie/beego/logs"
)

var (
	defaultLog *logs.BeeLogger
	log        *logs.BeeLogger
)

func init() {
	defaultLog = logs.NewLogger(10000)
	defaultLog.SetLogger("console", "")
	log = defaultLog
}

// Config 日志配置函数
func Config(conf LogConfig) {
	config(conf)
}

func config(conf LogConfig) {
	log = logs.NewLogger(conf.CacheSize)

	log.EnableFuncCallDepth(conf.EnableFuncCallDepth)
	log.SetLogFuncCallDepth(conf.FuncCallDepth)

	jsonString, err := json.Marshal(conf.Engine.Config)
	if err != nil {
		panic(err)
	}
	log.SetLogger(conf.Engine.Adapter, string(jsonString))
}

// Trace 打印trace级别日志
func Trace(format string, args ...interface{}) {
	log.Trace(format, args...)
}

// Debug 打印debug级别日志
func Debug(format string, args ...interface{}) {
	log.Debug(format, args...)
}

// Info 打印info级别日志
func Info(format string, args ...interface{}) {
	log.Info(format, args...)
}

// Warn 打印warn级别日志
func Warn(format string, args ...interface{}) {
	log.Warn(format, args...)
}

// Error 打印error级别日志
func Error(format string, args ...interface{}) {
	log.Error(format, args...)
}

// Fatal 打印critial级别日志
func Fatal(format string, args ...interface{}) {
	log.Critical(format, args...)
}
