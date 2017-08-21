package logger

import (
	"testing"
)

func TestConsoleLog(t *testing.T) {
	Trace("this is a test")
	Debug("this is a test")
	Info("this is a test")
	Warn("this is a test")
	Error("this is a test")
	Fatal("this is a test")

	Tracef("this is a %s", "test")
	Debugf("this is a %s", "test")
	Infof("this is a %s", "test")
	Warnf("this is a %s", "test")
	Errorf("this is a %s", "test")
	Fatalf("this is a %s", "test")
}

func TestFileLog(t *testing.T) {
	ConfigAsFile("seelog.xml")

	Trace("this is a test")
	Debug("this is a test")
	Info("this is a test")
	Warn("this is a test")
	Error("this is a test")
	Fatal("this is a test")

	Tracef("this is a %s", "test")
	Debugf("this is a %s", "test")
	Infof("this is a %s", "test")
	Warnf("this is a %s", "test")
	Errorf("this is a %s", "test")
	Fatalf("this is a %s", "test")
}
