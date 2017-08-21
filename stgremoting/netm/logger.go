package netm

import "git.oschina.net/cloudzone/smartgo/stgcommon/logger"

// Fatalf logs a fatal error
func (bootstrap *Bootstrap) Fatalf(format string, v ...interface{}) {
	//s.executeLogCall(func(logger Logger, format string, v ...interface{}) {
	//	logger.Fatalf(format, v...)
	//}, format, v...)
	logger.Fatal(format, v...)
}

// Debugf logs a debug statement
func (bootstrap *Bootstrap) Debugf(format string, v ...interface{}) {
	logger.Debug(format, v...)
}

// Noticef logs a notice statement
func (bootstrap *Bootstrap) Noticef(format string, v ...interface{}) {
	logger.Info(format, v...)
}
