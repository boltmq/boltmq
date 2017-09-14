package netm

import (
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-errors/errors"
)

// Bootstrap 启动器
type Bootstrap struct {
	listener         net.Listener
	mu               sync.Mutex
	contextTable     map[string]Context
	contextTableLock sync.RWMutex
	opts             *Options
	optsMu           sync.RWMutex
	handlers         []Handler
	keepalive        bool
	running          bool
	grRunning        bool
}

// NewBootstrap 创建启动器
func NewBootstrap() *Bootstrap {
	b := &Bootstrap{
		opts:      &Options{},
		grRunning: true,
	}
	b.contextTable = make(map[string]Context)

	return b
}

// Bind 监听地址、端口
func (bootstrap *Bootstrap) Bind(host string, port int) *Bootstrap {
	bootstrap.optsMu.Lock()
	bootstrap.opts.Host = host
	bootstrap.opts.Port = port
	bootstrap.optsMu.Unlock()
	return bootstrap
}

// Sync 启动服务
func (bootstrap *Bootstrap) Sync() {
	opts := bootstrap.getOpts()
	addr := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))

	listener, e := net.Listen("tcp", addr)
	if e != nil {
		bootstrap.Fatalf("Error listening on port: %s, %q", addr, e)
		return
	}
	bootstrap.Noticef("Listening for connections on %s",
		net.JoinHostPort(opts.Host, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)))
	bootstrap.Noticef("Bootstrap is ready")

	bootstrap.mu.Lock()
	if opts.Port == 0 {
		// Write resolved port back to options.
		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			bootstrap.Fatalf("Error parsing server address (%s): %s", listener.Addr().String(), err)
			bootstrap.mu.Unlock()
			return
		}
		portNum, err := strconv.Atoi(port)
		if err != nil {
			bootstrap.Fatalf("Error parsing server address (%s): %s", listener.Addr().String(), err)
			bootstrap.mu.Unlock()
			return
		}
		opts.Port = portNum
	}
	bootstrap.listener = listener
	bootstrap.running = true
	bootstrap.mu.Unlock()

	tmpDelay := ACCEPT_MIN_SLEEP
	for bootstrap.isRunning() {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				bootstrap.Debugf("Temporary Client Accept Error(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
				continue
			} else if bootstrap.isRunning() {
				bootstrap.Errorf("Accept error: %v", err)
				continue
			} else {
				bootstrap.Errorf("Exiting: %v", err)
				bootstrap.LogFlush()
				break
			}
		}
		tmpDelay = ACCEPT_MIN_SLEEP

		// 配置连接
		err = bootstrap.setConnect(conn)
		if err != nil {
			bootstrap.Errorf("config connect error: %v", err)
			continue
		}

		// 以客户端ip,port管理连接
		remoteAddr := conn.RemoteAddr().String()
		ctx := newDefaultContext(remoteAddr, conn)
		bootstrap.contextTableLock.Lock()
		bootstrap.contextTable[remoteAddr] = ctx
		bootstrap.contextTableLock.Unlock()
		bootstrap.Debugf("Client connection created %s", remoteAddr)

		bootstrap.startGoRoutine(func() {
			bootstrap.handleConn(remoteAddr, ctx)
		})
	}

	bootstrap.Noticef("Bootstrap Exiting..")
}

// Connect 连接指定地址、端口(服务器地址管理连接)
func (bootstrap *Bootstrap) Connect(host string, port int) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	return bootstrap.ConnectJoinAddr(addr)
}

// Connect 使用指定地址、端口的连接字符串连接
func (bootstrap *Bootstrap) ConnectJoinAddr(addr string) error {
	_, err := bootstrap.ConnectJoinAddrAndReturn(addr)
	return err
}

// Connect 使用指定地址、端口的连接字符串进行连接并返回连接
func (bootstrap *Bootstrap) ConnectJoinAddrAndReturn(addr string) (Context, error) {
	bootstrap.contextTableLock.RLock()
	ctx, ok := bootstrap.contextTable[addr]
	bootstrap.contextTableLock.RUnlock()
	if ok {
		return ctx, nil
	}

	nctx, e := bootstrap.connect(addr)
	if e != nil {
		bootstrap.Fatalf("Error Connect on port: %s, %q", addr, e)
		return nil, errors.Wrap(e, 0)
	}

	bootstrap.contextTableLock.Lock()
	bootstrap.contextTable[addr] = nctx
	bootstrap.contextTableLock.Unlock()
	bootstrap.Noticef("Connect listening on port: %s", addr)
	bootstrap.Noticef("client connections on %s", nctx.LocalAddr().String())

	bootstrap.startGoRoutine(func() {
		bootstrap.handleConn(addr, nctx)
	})

	return nctx, nil
}

// 创建新连接
func (bootstrap *Bootstrap) connect(addr string) (Context, error) {
	conn, e := net.Dial("tcp", addr)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	e = bootstrap.setConnect(conn)
	if e != nil {
		return nil, e
	}

	ctx := newDefaultContext(addr, conn)
	return ctx, nil
}

// HasConnect find connect by addr, return bool
func (bootstrap *Bootstrap) HasConnect(addr string) bool {
	bootstrap.contextTableLock.RLock()
	_, ok := bootstrap.contextTable[addr]
	bootstrap.contextTableLock.RUnlock()
	if !ok {
		return false
	}

	return true
}

// Disconnect 关闭指定连接
func (bootstrap *Bootstrap) Disconnect(addr string) {
	bootstrap.contextTableLock.RLock()
	ctx, ok := bootstrap.contextTable[addr]
	bootstrap.contextTableLock.RUnlock()
	if ok {
		bootstrap.disconnect(addr, ctx)
	}
}

// 关闭连接
func (bootstrap *Bootstrap) disconnect(addr string, ctx Context) {
	ctx.Close()
	bootstrap.contextTableLock.Lock()
	delete(bootstrap.contextTable, addr)
	bootstrap.contextTableLock.Unlock()
}

// Shutdown 关闭bootstrap
func (bootstrap *Bootstrap) Shutdown() {
	bootstrap.mu.Lock()
	bootstrap.running = false
	bootstrap.mu.Unlock()

	// 关闭listener
	bootstrap.listener.Close()

	// 关闭所有连接
	bootstrap.contextTableLock.Lock()
	for addr, ctx := range bootstrap.contextTable {
		ctx.Close()
		delete(bootstrap.contextTable, addr)
	}
	bootstrap.contextTableLock.Unlock()
}

// Write 发送消息
func (bootstrap *Bootstrap) Write(addr string, buffer []byte) (n int, e error) {
	bootstrap.contextTableLock.RLock()
	ctx, ok := bootstrap.contextTable[addr]
	bootstrap.contextTableLock.RUnlock()
	if !ok {
		bootstrap.Fatalf("not found connect: %s", addr)
		e = errors.Errorf("not found connect %s", addr)
		return
	}

	n, e = ctx.Write(buffer)
	if e != nil {
		bootstrap.disconnect(addr, ctx)
		e = errors.Wrap(e, 0)
	}

	return
}

// RegisterHandler 注册连接接收数据时回调执行函数
func (bootstrap *Bootstrap) RegisterHandler(fns ...Handler) *Bootstrap {
	bootstrap.handlers = append(bootstrap.handlers, fns...)
	return bootstrap
}

// 接收数据
func (bootstrap *Bootstrap) handleConn(addr string, ctx Context) {
	b := make([]byte, 1024)
	for {
		n, err := ctx.Read(b)
		if err != nil {
			bootstrap.disconnect(addr, ctx)
			bootstrap.Fatalf("failed handle connect: %s %s", addr, err)
			break
		}

		for _, fn := range bootstrap.handlers {
			fn(b[:n], ctx)
		}
	}

	bootstrap.Noticef("Connect[%s] Exiting..", addr)
}

func (bootstrap *Bootstrap) startGoRoutine(fn func()) {
	if bootstrap.grRunning {
		go fn()
	}
}

func (bootstrap *Bootstrap) isRunning() bool {
	bootstrap.mu.Lock()
	defer bootstrap.mu.Unlock()
	return bootstrap.running
}

func (bootstrap *Bootstrap) getOpts() *Options {
	bootstrap.optsMu.RLock()
	opts := bootstrap.opts
	bootstrap.optsMu.RUnlock()
	return opts
}

// Size 当前连接数
func (bootstrap *Bootstrap) Size() int {
	bootstrap.contextTableLock.RLock()
	defer bootstrap.contextTableLock.RUnlock()
	return len(bootstrap.contextTable)
}

// NewRandomConnect 连接指定地址、端口(客户端随机端口地址管理连接)。特殊业务使用
func (bootstrap *Bootstrap) NewRandomConnect(host string, port int) (Context, error) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	nctx, e := bootstrap.connect(addr)
	if e != nil {
		bootstrap.Fatalf("Error Connect on port: %s, %q", addr, e)
		return nil, errors.Wrap(e, 0)
	}

	localAddr := nctx.LocalAddr().String()
	bootstrap.contextTableLock.Lock()
	bootstrap.contextTable[localAddr] = nctx
	bootstrap.contextTableLock.Unlock()
	bootstrap.Noticef("Connect listening on port: %s", addr)
	bootstrap.Noticef("client connections on %s", localAddr)

	bootstrap.startGoRoutine(func() {
		bootstrap.handleConn(addr, nctx)
	})

	return nctx, nil
}

// SetKeepAlive 配置连接keepalive，default is false
func (bootstrap *Bootstrap) SetKeepAlive(keepalive bool) *Bootstrap {
	bootstrap.keepalive = keepalive
	return bootstrap
}

// 配置连接
func (bootstrap *Bootstrap) setConnect(conn net.Conn) error {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(bootstrap.keepalive); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	return nil
}
