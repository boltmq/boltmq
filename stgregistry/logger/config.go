package logger

import (
	"encoding/json"
	"github.com/astaxie/beego/logs"
)

var (
	defaultLog *logs.BeeLogger
	logger     *logs.BeeLogger
)

func init() {
	defaultLog = logs.NewLogger(10000)
	defaultLog.SetLogger("console", "")
	logger = defaultLog
}

// SetLog 日志函数
func SetLog(log *logs.BeeLogger) {
	logger = log
}

// SetConfig 配置
func SetConfig(conf Config) {
	logger = logs.NewLogger(conf.CacheSize)
	logger.EnableFuncCallDepth(conf.EnableFuncCallDepth)
	logger.SetLogFuncCallDepth(conf.FuncCallDepth)

	jsonString, err := json.Marshal(conf.Engine.Config)
	if err != nil {
		panic(err)
	}
	// 配置参数链接： http://blog.csdn.net/huwh_/article/details/77923570
	logger.SetLogger(conf.Engine.Adapter, string(jsonString))
}

// Trace 打印trace级别日志
func Trace(format string, args ...interface{}) {
	logger.Trace(format, args...)
}

// Debug 打印debug级别日志
func Debug(format string, args ...interface{}) {
	logger.Debug(format, args...)
}

// Info 打印info级别日志
func Info(format string, args ...interface{}) {
	logger.Info(format, args...)
}

// Warn 打印warn级别日志
func Warn(format string, args ...interface{}) {
	logger.Warn(format, args...)
}

// Error 打印error级别日志
func Error(format string, args ...interface{}) {
	logger.Error(format, args...)
}

// Fatal 打印critial级别日志
func Fatal(format string, args ...interface{}) {
	logger.Critical(format, args...)
}
