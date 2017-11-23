package core

import (
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-errors/errors"
)

// Bootstrap 启动器
type Bootstrap struct {
	listener      net.Listener
	eventListener EventListener
	opts          *Options
	handlers      []Handler
	running       bool
	runningMu     sync.Mutex
}

// NewBootstrap 创建启动器
func NewBootstrap() *Bootstrap {
	b := &Bootstrap{
		opts:          &Options{ReadBufferSize: READ_BUFFER_SIZE},
		eventListener: &defaultEventListener{},
	}
	return b
}

// Bind 监听地址、端口
func (bootstrap *Bootstrap) Bind(host string, port int) *Bootstrap {
	bootstrap.opts.Host = host
	bootstrap.opts.Port = port
	return bootstrap
}

// Sync 启动服务
func (bootstrap *Bootstrap) Sync() {
	// check handlers if register
	if len(bootstrap.handlers) == 0 {
		bootstrap.Fatalf("no handler register, data not process.")
	}

	opts := bootstrap.opts
	addr := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))

	listener, e := net.Listen("tcp", addr)
	if e != nil {
		bootstrap.Fatalf("Error listening on port: %s, %q", addr, e)
		return
	}
	bootstrap.Noticef("listening for connections on %s", net.JoinHostPort(opts.Host, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)))
	bootstrap.Noticef("bootstrap is ready")

	bootstrap.runningMu.Lock()
	if opts.Port == 0 {
		// Write resolved port back to options.
		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			bootstrap.Fatalf("Error parsing server address (%s): %s", listener.Addr().String(), err)
			bootstrap.runningMu.Unlock()
			return
		}
		portNum, err := strconv.Atoi(port)
		if err != nil {
			bootstrap.Fatalf("Error parsing server address (%s): %s", listener.Addr().String(), err)
			bootstrap.runningMu.Unlock()
			return
		}
		opts.Port = portNum
	}
	bootstrap.listener = listener
	bootstrap.running = true
	bootstrap.runningMu.Unlock()

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

		bootstrap.startGoRoutine(func() {
			// 事件通知-客户端连接
			bootstrap.eventListener.OnContextActive(conn)
			bootstrap.handleContext(conn)
		})
	}

	bootstrap.Noticef("Bootstrap Exiting..")
}

// 连接接收数据
func (bootstrap *Bootstrap) handleContext(ctx net.Conn) {
	var (
		n int
		e error
		b = make([]byte, bootstrap.opts.ReadBufferSize)
	)

	for {
		n, e = ctx.Read(b)
		if e != nil {
			break
		}

		for _, fn := range bootstrap.handlers {
			fn(b[:n], ctx)
		}
	}

	// 判断e确定事件通知
	if e == io.EOF {
		bootstrap.eventListener.OnContextClosed(ctx)
	} else {
		bootstrap.eventListener.OnContextError(ctx, e)
	}
}

// Connect 连接服务器连接地址、端口，使用随机端口连接
func (bootstrap *Bootstrap) Connect(sraddr string) error {
	return bootstrap.ConnectUseInterface(sraddr, "")
}

// ConnectUseInterface 连接服务器连接地址、端口，使用指定网络接口连接(laddr端口为0使用随机端口)。
func (bootstrap *Bootstrap) ConnectUseInterface(sraddr, sladdr string) error {
	// check handlers if register
	if len(bootstrap.handlers) == 0 {
		bootstrap.Warnf("no handler register, data not process.")
	}

	ctx, e := bootstrap.connect(sraddr, sladdr)
	if e != nil {
		//bootstrap.Fatalf("Error Connect on port: %s, %q", sraddr, e)
		return errors.Wrap(e, 0)
	}

	bootstrap.startGoRoutine(func() {
		// 事件通知-创建连接
		bootstrap.eventListener.OnContextConnect(ctx)
		bootstrap.handleContext(ctx)
	})

	return nil
}

// 创建新连接
func (bootstrap *Bootstrap) connect(sraddr, sladdr string) (net.Conn, error) {
	raddr, e := net.ResolveTCPAddr("tcp", sraddr)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	var laddr *net.TCPAddr
	if sladdr != "" {
		laddr, e = net.ResolveTCPAddr("tcp", sladdr)
		if e != nil {
			return nil, errors.Wrap(e, 0)
		}
	}

	conn, e := net.DialTCP("tcp", laddr, raddr)
	if e != nil {
		return nil, errors.Wrap(e, 0)
	}

	e = bootstrap.setConnect(conn)
	if e != nil {
		return nil, e
	}

	//ctx := newDefaultContext(sraddr, conn, bootstrap)
	return conn, nil
}

// RegisterHandler 注册连接接收数据时回调执行函数
func (bootstrap *Bootstrap) RegisterHandler(fns ...Handler) *Bootstrap {
	bootstrap.handlers = append(bootstrap.handlers, fns...)
	return bootstrap
}

// SetEventListener 设置连接的事件监听
func (bootstrap *Bootstrap) SetEventListener(eventListener EventListener) *Bootstrap {
	bootstrap.eventListener = eventListener
	return bootstrap
}

func (bootstrap *Bootstrap) startGoRoutine(fn func()) {
	go fn()
}

func (bootstrap *Bootstrap) isRunning() bool {
	bootstrap.runningMu.Lock()
	defer bootstrap.runningMu.Unlock()
	return bootstrap.running
}

// SetKeepAlive 配置连接keepalive，default is false
func (bootstrap *Bootstrap) SetKeepAlive(keepalive bool) *Bootstrap {
	bootstrap.opts.Keepalive = keepalive
	return bootstrap
}

// SetReadBufferSize 配置连接读缓存大小。
func (bootstrap *Bootstrap) SetReadBufferSize(readBufferSize int) *Bootstrap {
	bootstrap.opts.ReadBufferSize = readBufferSize
	return bootstrap
}

// 配置连接
func (bootstrap *Bootstrap) setConnect(conn net.Conn) error {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(bootstrap.opts.Keepalive); err != nil {
			return errors.Wrap(err, 0)
		}
	}

	return nil
}
