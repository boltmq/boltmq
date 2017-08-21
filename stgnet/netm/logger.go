package netm

import "git.oschina.net/cloudzone/smartgo/stgcommon/logger"

// Debugf logs a debug statement
func (bootstrap *Bootstrap) Debugf(format string, v ...interface{}) {
	logger.Debugf(format, v...)
}

// Tracef logs a trace statement
func (bootstrap *Bootstrap) Tracef(format string, v ...interface{}) {
	logger.Tracef(format, v...)
}

// Noticef logs a notice statement
func (bootstrap *Bootstrap) Noticef(format string, v ...interface{}) {
	logger.Infof(format, v...)
}

// Errorf logs an error
func (bootstrap *Bootstrap) Errorf(format string, v ...interface{}) {
	logger.Errorf(format, v...)
}

// Fatalf logs a fatal error
func (bootstrap *Bootstrap) Fatalf(format string, v ...interface{}) {
	logger.Fatalf(format, v...)
}

// LogFlush logs flush
func (bootstrap *Bootstrap) LogFlush() {
	logger.Flush()
}
