package netm

import (
	"fmt"
	"io"
	"net"
	"time"
)

// Context the context of connection, like conn channel, not go chan.
type Context interface {
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	WriteSerialObject(s Serializable) (n int, e error)
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Close() error
	IsClosed() bool
	Idle() time.Duration
	Addr() string
	ToString() string
}

// DefaultContext default context
type DefaultContext struct {
	addr        string
	conn        net.Conn
	bootstrap   *Bootstrap
	lastOptTime time.Time
	isClosed    bool
}

// 创建一个连接context
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
	ctx.lastOptTime = time.Now()

	return
}

// Write 写数据
func (ctx *DefaultContext) Write(b []byte) (n int, e error) {
	n, e = ctx.conn.Write(b)
	if e != nil {
		ctx.onError(e)
	}
	ctx.lastOptTime = time.Now()

	return
}

// WriteSerialObject 写序列化数据
func (ctx *DefaultContext) WriteSerialObject(s Serializable) (n int, e error) {
	if s == nil {
		return
	}

	return ctx.Write(s.Bytes())
}

// Close 关闭连接
func (ctx *DefaultContext) Close() error {
	if ctx.isClosed {
		return nil
	}

	ctx.bootstrap.onContextClose(ctx)
	e := ctx.conn.Close()
	ctx.isClosed = true

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

// IsClosed 返回索引地址
func (ctx *DefaultContext) IsClosed() bool {
	return ctx.isClosed
}

// Idle 返回空闲时间
func (ctx *DefaultContext) Idle() time.Duration {
	return time.Since(ctx.lastOptTime)
}

// onError 错误通知
func (ctx *DefaultContext) onError(e error) {
	if e == io.EOF {
		ctx.bootstrap.onContextClose(ctx)
		fmt.Printf("bootstrap.onContextClose() ----> \n")
	} else {
		fmt.Printf("bootstrap.onContextError() ----> %s\n", e.Error())
		ctx.bootstrap.onContextError(ctx)
	}
	ctx.isClosed = true

	ctx.conn.Close()
}

// ToString 打印net.Conn的基本参数
func (ctx *DefaultContext) ToString() string {
	if ctx == nil || ctx.conn == nil {
		return fmt.Sprintf("ctx or ctx.conn is nil")
	}

	format := "net.conn [localAddr=%s, remoteAddr=%s, addr=%s, isClosed=%t]"
	return fmt.Sprintf(format, ctx.conn.LocalAddr().String(), ctx.conn.RemoteAddr().String(), ctx.addr, ctx.isClosed)
}
