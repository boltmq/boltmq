package core

import "testing"

func TestConsoleLog(t *testing.T) {

	bootstrap := NewBootstrap()
	bootstrap.Trace("this is a test")
	bootstrap.Debug("this is a test")
	bootstrap.Trace("this is a test")
	bootstrap.Warn("this is a test")
	bootstrap.Error("this is a test")
	bootstrap.Fatal("this is a test")

	bootstrap.Tracef("this is a %s", "test")
	bootstrap.Debugf("this is a %s", "test")
	bootstrap.Tracef("this is a %s", "test")
	bootstrap.Warnf("this is a %s", "test")
	bootstrap.Errorf("this is a %s", "test")
	bootstrap.Fatalf("this is a %s", "test")
}
