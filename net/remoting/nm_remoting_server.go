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
