package logger

import "github.com/cihub/seelog"

// Trace 打印trace级别日志
func Trace(args ...interface{}) {
	seelog.Trace(args...)
}

// Debug 打印debug级别日志
func Debug(args ...interface{}) {
	seelog.Debug(args...)
}

// Info 打印info级别日志
func Info(args ...interface{}) {
	seelog.Info(args...)
}

// Warn 打印warn级别日志
func Warn(args ...interface{}) {
	seelog.Warn(args...)
}

// Error 打印error级别日志
func Error(args ...interface{}) {
	seelog.Error(args...)
}

// Fatal 打印critial级别日志
func Fatal(args ...interface{}) {
	seelog.Critical(args...)
}

// Trace 打印trace级别日志
func Tracef(format string, args ...interface{}) {
	seelog.Tracef(format, args...)
}

// Debug 打印debug级别日志
func Debugf(format string, args ...interface{}) {
	seelog.Debugf(format, args...)
}

// Info 打印info级别日志
func Infof(format string, args ...interface{}) {
	seelog.Infof(format, args...)
}

// Warn 打印warn级别日志
func Warnf(format string, args ...interface{}) {
	seelog.Warnf(format, args...)
}

// Error 打印error级别日志
func Errorf(format string, args ...interface{}) {
	seelog.Errorf(format, args...)
}

// Fatal 打印critial级别日志
func Fatalf(format string, args ...interface{}) {
	seelog.Criticalf(format, args...)
}

// Flush 刷新日志
func Flush() {
	seelog.Flush()
}

// ConfigAsFile 配置文件
func ConfigAsFile(filename string) error {
	logger, err := seelog.LoggerFromConfigAsFile(filename)
	if err != nil {
		return err
	}

	seelog.ReplaceLogger(logger)
	return nil
}
