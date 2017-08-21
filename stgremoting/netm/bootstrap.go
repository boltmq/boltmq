package netm

import (
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
	running     bool
}

// NewBootstrap 创建启动器
func NewBootstrap() *Bootstrap {
	b := &Bootstrap{
		opts: &Options{},
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
	}

	bootstrap.Noticef("Bootstrap Exiting..")
}

// Connect 连接指定地址、端口
func (bootstrap *Bootstrap) Connect(host string, port int) error {
	addr := net.JoinHostPort(host, strconv.Itoa(port))

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
		bootstrap.Noticef("connect on port: %s", addr)
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
