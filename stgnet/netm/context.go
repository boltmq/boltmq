package netm

import "net"

// Context the context of connection, like conn channel, not go chan.
type Context interface {
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Close() error
	Addr() string
}

// DefaultContext default context
type DefaultContext struct {
	addr string
	conn net.Conn
}

func newDefaultContext(addr string, conn net.Conn) *DefaultContext {
	return &DefaultContext{
		addr: addr,
		conn: conn,
	}
}

// Read 读取数据
func (ctx *DefaultContext) Read(b []byte) (n int, err error) {
	return ctx.conn.Read(b)
}

// Write 写数据
func (ctx *DefaultContext) Write(b []byte) (n int, err error) {
	return ctx.conn.Write(b)
}

// Close 关闭连接
func (ctx *DefaultContext) Close() error {
	return ctx.conn.Close()
}

// LocalAddr 本地连接地址
func (ctx *DefaultContext) LocalAddr() net.Addr {
	return ctx.conn.LocalAddr()
}

// RemoteAddr 远程连接地址
func (ctx *DefaultContext) RemoteAddr() net.Addr {
	return ctx.conn.RemoteAddr()
}

// Addr 返回索引地址
func (ctx *DefaultContext) Addr() string {
	return ctx.addr
}
