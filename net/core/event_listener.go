package core

import (
	"net"

	"github.com/boltmq/common/logger"
)

type EventListener interface {
	OnContextActive(ctx net.Conn)
	OnContextConnect(ctx net.Conn)
	OnContextClosed(ctx net.Conn)
	OnContextError(ctx net.Conn, err error)
}

type defaultEventListener struct {
}

func (listener *defaultEventListener) OnContextActive(ctx net.Conn) {
	logger.Infof("OnContextActive: Connection active, %s.", ctx.RemoteAddr().String())
}

func (listener *defaultEventListener) OnContextConnect(ctx net.Conn) {
	logger.Infof("OnContextConnect: Client %s connect to %s.", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *defaultEventListener) OnContextClosed(ctx net.Conn) {
	logger.Infof("OnContextClosed: local %s Exiting, Remote %s.", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *defaultEventListener) OnContextError(ctx net.Conn, err error) {
	logger.Errorf("OnContextError: local %s, Remote %s, err: %v.", ctx.LocalAddr().String(), ctx.RemoteAddr().String(), err)
}
