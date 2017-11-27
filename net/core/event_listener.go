package core

import (
	"github.com/boltmq/common/logger"
)

type EventListener interface {
	OnContextActive(ctx Context)
	OnContextConnect(ctx Context)
	OnContextClosed(ctx Context)
	OnContextError(ctx Context, err error)
}

type DefaultEventListener struct {
}

func (listener *DefaultEventListener) OnContextActive(ctx Context) {
	logger.Infof("OnContextActive: Connection active, %s.", ctx.RemoteAddr().String())
}

func (listener *DefaultEventListener) OnContextConnect(ctx Context) {
	logger.Infof("OnContextConnect: Client %s connect to %s.", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *DefaultEventListener) OnContextClosed(ctx Context) {
	logger.Infof("OnContextClosed: local %s Exiting, Remote %s.", ctx.LocalAddr().String(), ctx.RemoteAddr().String())
}

func (listener *DefaultEventListener) OnContextError(ctx Context, err error) {
	logger.Errorf("OnContextError: local %s, Remote %s, err: %v.", ctx.LocalAddr().String(), ctx.RemoteAddr().String(), err)
}
