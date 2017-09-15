package netm

import (
	"io"
	"net"
)

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
	addr      string
	conn      net.Conn
	bootstrap *Bootstrap
}

func newDefaultContext(addr string, conn net.Conn, bootstrap *Bootstrap) *DefaultContext {
	return &DefaultContext{
		addr:      addr,
		conn:      conn,
		bootstrap: bootstrap,
	}
}

// Read 读取数据
func (ctx *DefaultContext) Read(b []byte) (n int, e error) {
	n, e = ctx.conn.Read(b)
	if e != nil {
		ctx.onError(e)
	}

	return
}

// Write 写数据
func (ctx *DefaultContext) Write(b []byte) (n int, e error) {
	n, e = ctx.conn.Write(b)
	if e != nil {
		ctx.onError(e)
	}

	return
}

// Close 关闭连接
func (ctx *DefaultContext) Close() error {
	ctx.bootstrap.onContextClose(ctx)
	e := ctx.conn.Close()

	return e
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

// 错误通知
func (ctx *DefaultContext) onError(e error) {
	if e == io.EOF {
		ctx.bootstrap.onContextClose(ctx)
	} else {
		ctx.bootstrap.onContextError(ctx)
	}

	ctx.conn.Close()
}
