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
package remoting

import (
	"github.com/boltmq/boltmq/net/core"
	"github.com/boltmq/common/protocol"
)

// NMRemotingServer net remoting server
type NMRemotingServer struct {
	host string
	port int
	BaseRemotingAchieve
}

// NewNMRemotingServer return new default remoting server
func NewNMRemotingServer(host string, port int) *NMRemotingServer {
	remotingServe := &NMRemotingServer{
		host: host,
		port: port,
	}
	remotingServe.responseTable = make(map[int32]*ResponseFuture)
	remotingServe.fragmentationActuator = NewLengthFieldFragmentationAssemblage(FRAME_MAX_LENGTH, 0, 4, 0)
	remotingServe.bootstrap = core.NewBootstrap()
	return remotingServe
}

// Start start server
func (rs *NMRemotingServer) Start() {
	rs.bootstrap.RegisterHandler(func(buffer []byte, ctx core.Context) {
		rs.processReceived(buffer, ctx)
	})

	rs.isRunning = true
	// 定时扫描响应
	rs.startScheduledTask()

	// 启动服务
	rs.bootstrap.Bind(rs.host, rs.port).Sync()
}

// Shutdown shutdown server
func (rs *NMRemotingServer) Shutdown() {
	if rs.timeoutTimer != nil {
		rs.timeoutTimer.Stop()
	}

	if rs.bootstrap != nil {
		rs.bootstrap.Shutdown()
	}
	rs.isRunning = false
}

// InvokeSync 同步调用并返回响应, addr为空字符串
func (rs *NMRemotingServer) InvokeSync(ctx core.Context, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	//addr := ctx.RemoteAddr().String()
	return rs.invokeSync(ctx, request, timeoutMillis)
}

// InvokeAsync 异步调用
func (rs *NMRemotingServer) InvokeAsync(ctx core.Context, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	//addr := ctx.RemoteAddr().String()
	return rs.invokeAsync(ctx, request, timeoutMillis, invokeCallback)
}

// InvokeSync 单向发送消息
func (rs *NMRemotingServer) InvokeOneway(ctx core.Context, request *protocol.RemotingCommand, timeoutMillis int64) error {
	//addr := ctx.RemoteAddr().String()
	return rs.invokeOneway(ctx, request, timeoutMillis)
}

func (rs *NMRemotingServer) ListenPort() int {
	return rs.port
}
