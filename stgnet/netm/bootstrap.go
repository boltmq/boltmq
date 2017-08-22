package netm

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

// Bootstrap 启动器
type Bootstrap struct {
	listener    net.Listener
	mu          sync.Mutex
	connTable   map[string]net.Conn
	connTableMu sync.RWMutex
	opts        *Options
	optsMu      sync.RWMutex
	handlers    []Handler
	running     bool
	grRunning   bool
}

// NewBootstrap 创建启动器
func NewBootstrap() *Bootstrap {
	b := &Bootstrap{
		opts:      &Options{},
		grRunning: true,
	}
	b.connTable = make(map[string]net.Conn)
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
	bootstrap.Noticef("Listening for client connections on %s",
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
			} else if bootstrap.isRunning() {
				bootstrap.Noticef("Accept error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP

		remoteAddr := conn.RemoteAddr().String()
		bootstrap.connTableMu.Lock()
		bootstrap.connTable[remoteAddr] = conn
		bootstrap.connTableMu.Unlock()
		bootstrap.Debugf("Client connection created %s", remoteAddr)

		bootstrap.startGoRoutine(func() {
			bootstrap.handleConn(remoteAddr, conn)
		})
	}

	bootstrap.Noticef("Bootstrap Exiting..")
}

// Connect 连接指定地址、端口
func (bootstrap *Bootstrap) Connect(host string, port int) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	return bootstrap.ConnectJoinAddr(addr)
}

// Connect 使用指定地址、端口的连接字符串连接
func (bootstrap *Bootstrap) ConnectJoinAddr(addr string) error {

	bootstrap.connTableMu.RLock()
	_, ok := bootstrap.connTable[addr]
	bootstrap.connTableMu.RUnlock()
	if !ok {
		conn, e := bootstrap.connect(addr)
		if e != nil {
			bootstrap.Fatalf("Error Connect on port: %s, %q", addr, e)
			return e
		}

		bootstrap.connTableMu.Lock()
		bootstrap.connTable[addr] = conn
		bootstrap.connTableMu.Unlock()
		bootstrap.Noticef("Connect listening on port: %s", addr)
		bootstrap.Noticef("client connections on %s", conn.LocalAddr().String())

		bootstrap.startGoRoutine(func() {
			bootstrap.handleConn(addr, conn)
		})
	}

	return nil
}

func (bootstrap *Bootstrap) connect(addr string) (net.Conn, error) {
	conn, e := net.Dial("tcp", addr)
	if e != nil {
		return nil, e
	}

	return conn, nil
}

// HasConnect find connect by addr, return bool
func (bootstrap *Bootstrap) HasConnect(addr string) bool {
	bootstrap.connTableMu.RLock()
	_, ok := bootstrap.connTable[addr]
	bootstrap.connTableMu.RUnlock()
	if !ok {
		return false
	}

	return true
}

// Disconnect 关闭指定连接
func (bootstrap *Bootstrap) Disconnect(addr string) {
	bootstrap.connTableMu.RLock()
	conn, ok := bootstrap.connTable[addr]
	bootstrap.connTableMu.RUnlock()
	if ok {
		bootstrap.disconnect(addr, conn)
	}
}

func (bootstrap *Bootstrap) disconnect(addr string, conn net.Conn) {
	conn.Close()
	bootstrap.connTableMu.Lock()
	delete(bootstrap.connTable, addr)
	bootstrap.connTableMu.Unlock()
}

// Shutdown 关闭bootstrap
func (bootstrap *Bootstrap) Shutdown() {
	bootstrap.mu.Lock()
	bootstrap.running = false
	bootstrap.mu.Unlock()

	// 关闭所有连接
	bootstrap.connTableMu.Lock()
	for addr, conn := range bootstrap.connTable {
		conn.Close()
		delete(bootstrap.connTable, addr)
	}
	bootstrap.connTableMu.Unlock()
}

// Write 发送消息
func (bootstrap *Bootstrap) Write(addr string, buffer []byte) (n int, err error) {
	bootstrap.connTableMu.RLock()
	conn, ok := bootstrap.connTable[addr]
	bootstrap.connTableMu.RUnlock()
	if !ok {
		bootstrap.Fatalf("not found connect: %s", addr)
		err = fmt.Errorf("not found connect %s", addr)
		return
	}

	return bootstrap.write(addr, conn, buffer)
}

func (bootstrap *Bootstrap) write(addr string, conn net.Conn, buffer []byte) (n int, err error) {
	n, err = conn.Write(buffer)
	if err != nil {
		bootstrap.disconnect(addr, conn)
	}

	return
}

// RegisterHandler 注册连接接收数据时回调执行函数
func (bootstrap *Bootstrap) RegisterHandler(fns ...Handler) *Bootstrap {
	bootstrap.handlers = append(bootstrap.handlers, fns...)
	return bootstrap
}

func (bootstrap *Bootstrap) handleConn(addr string, conn net.Conn) {
	b := make([]byte, 1024)
	for {
		n, err := conn.Read(b)
		if err != nil {
			bootstrap.disconnect(addr, conn)
			bootstrap.Fatalf("failed handle connect: %s %s", addr, err)
			return
		}

		for _, fn := range bootstrap.handlers {
			fn(b[:n], addr, conn)
		}
	}
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
