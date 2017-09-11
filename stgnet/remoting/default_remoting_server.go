package remoting

import (
	"net"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"strconv"
)

// DefalutRemotingServer default remoting server
type DefalutRemotingServer struct {
	host      string
	port      int
	bootstrap *netm.Bootstrap
	BaseRemotingAchieve
}

// NewDefalutRemotingServer return new default remoting server
func NewDefalutRemotingServer(host string, port int) *DefalutRemotingServer {
	remotingServe := &DefalutRemotingServer{
		host: host,
		port: port,
	}
	remotingServe.responseTable = make(map[int32]*ResponseFuture)
	remotingServe.framePacketActuator = NewLengthFieldFramePacket(FRAME_MAX_LENGTH, 0, 4, 0)
	remotingServe.bootstrap = netm.NewBootstrap()
	return remotingServe
}

// Start start server
func (rs *DefalutRemotingServer) Start() {
	rs.bootstrap.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		rs.processReceived(buffer, addr, conn)
	})

	rs.isRunning = true
	// 定时扫描响应
	rs.startScheduledTask()

	// 启动服务
	rs.bootstrap.Bind(rs.host, rs.port).Sync()
}

// Shutdown shutdown server
func (rs *DefalutRemotingServer) Shutdown() {
	rs.timeoutTimer.Stop()
	rs.bootstrap.Shutdown()
	rs.isRunning = false
}

// InvokeSync 同步调用并返回响应, addr为空字符串
func (rs *DefalutRemotingServer) InvokeSync(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	addr := conn.RemoteAddr().String()
	return rs.invokeSync(addr, conn, request, timeoutMillis)
}

// InvokeAsync 异步调用
func (rs *DefalutRemotingServer) InvokeAsync(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	addr := conn.RemoteAddr().String()
	return rs.invokeAsync(addr, conn, request, timeoutMillis, invokeCallback)
}

// InvokeSync 单向发送消息
func (rs *DefalutRemotingServer) InvokeOneway(conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) error {
	addr := conn.RemoteAddr().String()
	return rs.invokeOneway(addr, conn, request, timeoutMillis)
}

// GetListenPort 获得监听端口
// Author rongzhihong
// Since 2017/9/5
func (rs *DefalutRemotingServer) GetListenPort() string {
	return strconv.Itoa(rs.port)
}
