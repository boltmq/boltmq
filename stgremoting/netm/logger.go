package netm

import "git.oschina.net/cloudzone/smartgo/stgcommon/logger"

// Fatalf logs a fatal error
func (bootstrap *Bootstrap) Fatalf(format string, v ...interface{}) {
	logger.Fatalf(format, v...)
}

// Debugf logs a debug statement
func (bootstrap *Bootstrap) Debugf(format string, v ...interface{}) {
	logger.Debugf(format, v...)
}

// Noticef logs a notice statement
func (bootstrap *Bootstrap) Noticef(format string, v ...interface{}) {
	logger.Infof(format, v...)
}

// LogFlush logs flush
func (bootstrap *Bootstrap) LogFlush() {
	logger.Flush()
}
