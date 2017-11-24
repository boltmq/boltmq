package core

import (
	"fmt"
	"net"
)

// Context the context of connection, like conn channel, not go chan.
type Context interface {
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	WriteSerialData(s Serializable) (n int, e error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	LocalAddrToSocketAddr() *SocketAddr
	RemoteAddrToSocketAddr() *SocketAddr
	Close() error
	IsClosed() bool
	String() string
}

type defaultContext struct {
	conn     net.Conn
	isClosed bool
}

// 创建一个连接context
func newDefaultContext(conn net.Conn) *defaultContext {
	return &defaultContext{
		conn: conn,
	}
}

// Read 读取数据
func (ctx *defaultContext) Read(b []byte) (n int, e error) {
	return ctx.conn.Read(b)
}

// Write 写数据
func (ctx *defaultContext) Write(b []byte) (n int, e error) {
	return ctx.conn.Write(b)
}

// WriteSerialData 写序列化数据
func (ctx *defaultContext) WriteSerialData(s Serializable) (n int, e error) {
	if s == nil {
		return
	}

	return ctx.conn.Write(s.Bytes())
}

// Close 关闭连接
func (ctx *defaultContext) Close() error {
	if ctx.isClosed {
		return nil
	}

	e := ctx.conn.Close()
	ctx.isClosed = true

	return e
}

// LocalAddr 本地连接地址
func (ctx *defaultContext) LocalAddr() net.Addr {
	return ctx.conn.LocalAddr()
}

// LocalAddrToSocketAddr 本地连接地址转为SocketAddr，SocketAddr是可比较的对象
func (ctx *defaultContext) LocalAddrToSocketAddr() (sa *SocketAddr) {
	localAddr := ctx.LocalAddr()
	tcpAddr, ok := localAddr.(*net.TCPAddr)
	if !ok {
		return nil
	}

	sa = &SocketAddr{
		Port: tcpAddr.Port,
	}
	copy(sa.IP[:], tcpAddr.IP[0:net.IPv4len])
	return
}

func (ctx *defaultContext) RemoteAddrToSocketAddr() (sa *SocketAddr) {
	remoteAddr := ctx.RemoteAddr()
	tcpAddr, ok := remoteAddr.(*net.TCPAddr)
	if !ok {
		return nil
	}

	sa = &SocketAddr{
		Port: tcpAddr.Port,
	}
	copy(sa.IP[:], tcpAddr.IP[0:net.IPv4len])
	return
}

// RemoteAddr 远程连接地址
func (ctx *defaultContext) RemoteAddr() net.Addr {
	return ctx.conn.RemoteAddr()
}

// IsClosed 连接是否关闭
func (ctx *defaultContext) IsClosed() bool {
	return ctx.isClosed
}

// String 实现String接口
func (ctx *defaultContext) String() string {
	if ctx.conn == nil {
		return "ctx conn is nil"
	}

	return fmt.Sprintf("net.conn [localAddr=%s, remoteAddr=%s, isClosed=%t]",
		ctx.conn.LocalAddr().String(), ctx.conn.RemoteAddr().String(), ctx.isClosed)
}
