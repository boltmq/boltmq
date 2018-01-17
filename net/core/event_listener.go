// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
	logger.Infof("OnContextActive: Connection active, %s.", ctx.RemoteAddr())
}

func (listener *DefaultEventListener) OnContextConnect(ctx Context) {
	logger.Infof("OnContextConnect: Client %s connect to %s.", ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *DefaultEventListener) OnContextClosed(ctx Context) {
	logger.Infof("OnContextClosed: local %s Exiting, Remote %s.", ctx.LocalAddr(), ctx.RemoteAddr())
}

func (listener *DefaultEventListener) OnContextError(ctx Context, err error) {
	logger.Errorf("OnContextError: local %s, Remote %s, err: %s.", ctx.LocalAddr(), ctx.RemoteAddr(), err)
}
