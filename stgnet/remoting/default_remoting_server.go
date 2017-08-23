package remoting

import (
	"net"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// DefalutRemotingServer default remoting server
type DefalutRemotingServer struct {
	host      string
	port      int
	bootstrap *netm.Bootstrap
	BaseRemotingClient
}

// NewDefalutRemotingServer return new default remoting server
func NewDefalutRemotingServer(host string, port int) *DefalutRemotingServer {
	remotingClient := &DefalutRemotingServer{
		host: host,
		port: port,
	}
	remotingClient.responseTable = make(map[int32]*ResponseFuture)
	remotingClient.bootstrap = netm.NewBootstrap()
	return remotingClient
}

// Start start server
func (rc *DefalutRemotingServer) Start() {
	rc.bootstrap.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		rc.processReceived(buffer, addr, conn)
	})

	rc.isRunning = true
	// 定时扫描响应
	rc.startScheduledTask()

	// 启动服务
	rc.bootstrap.Bind(rc.host, rc.port).Sync()
}

// Shutdown shutdown server
func (rc *DefalutRemotingServer) Shutdown() {
	rc.timeoutTimer.Stop()
	rc.bootstrap.Shutdown()
	rc.isRunning = false
}

// InvokeSync 同步调用并返回响应, addr为空字符串
func (rc *DefalutRemotingServer) InvokeSync(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	addr := conn.RemoteAddr().String()
	return rc.invokeSync(addr, conn, request, timeoutMillis)
}

// InvokeAsync 异步调用
func (rc *DefalutRemotingServer) InvokeAsync(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	addr := conn.RemoteAddr().String()
	return rc.invokeAsync(addr, conn, request, timeoutMillis, invokeCallback)
}

// InvokeSync 单向发送消息
func (rc *DefalutRemotingServer) InvokeOneway(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) error {
	addr := conn.RemoteAddr().String()
	return rc.invokeOneway(addr, conn, request, timeoutMillis)
}
