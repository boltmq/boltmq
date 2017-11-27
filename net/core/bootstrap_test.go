package core

import (
	"sync"
	"testing"
)

type serveEventListener struct {
	DefaultEventListener
	contextsMu sync.RWMutex
	contexts   map[SocketAddr]Context
}

func (listener *serveEventListener) OnContextActive(ctx Context) {
	socketAddr := ctx.RemoteAddrToSocketAddr()
	if socketAddr == nil {
		return
	}

	listener.contextsMu.Lock()
	listener.contexts[*socketAddr] = ctx
	listener.contextsMu.Unlock()
}

func (listener *serveEventListener) CloseConntexts() {
	listener.contextsMu.Lock()
	for sa, ctx := range listener.contexts {
		ctx.Close()
		delete(listener.contexts, sa)
	}
	listener.contextsMu.Unlock()
}

type clientEventListener struct {
	DefaultEventListener
	context Context
}

func (listener *clientEventListener) OnContextConnect(ctx Context) {
	listener.context = ctx
}

func (listener *clientEventListener) Send(msg []byte) error {
	_, err := listener.context.Write(msg)
	return err
}

func TestClient2ServerBootstrap(t *testing.T) {
	var (
		cEventListener = &clientEventListener{}
		sEventListener = &serveEventListener{contexts: make(map[SocketAddr]Context)}
		cBootstrap     *Bootstrap
		sBootstrap     *Bootstrap
		sc             int64
		cc             int64
		wg             sync.WaitGroup
	)

	// Server
	go func() {
		sBootstrap = NewBootstrap().SetReadBufferSize(512).SetEventListener(sEventListener)
		sBootstrap.SetKeepAlive(false).Bind("0.0.0.0", 8000).
			RegisterHandler(func(buffer []byte, ctx Context) {
				sc++
				ctx.Write([]byte("hi, client"))
			}).Sync()
	}()

	wg.Add(1)
	// Client
	go func() {
		cBootstrap = NewBootstrap().SetReadBufferSize(512).SetEventListener(cEventListener)
		err := cBootstrap.RegisterHandler(func(buffer []byte, ctx Context) {
			cc++
			wg.Done()
		}).Connect("127.0.0.1:8000")
		if err != nil {
			t.Errorf("Connect: %v", err)
			return
		}

		err = cEventListener.Send([]byte("hello, server."))
		if err != nil {
			t.Errorf("Send: %v", err)
			return
		}
	}()

	wg.Wait()
	sBootstrap.Shutdown()
	sEventListener.CloseConntexts()
	cBootstrap.Shutdown()
	cEventListener.context.Close()

	if sc != 1 {
		t.Errorf("Server not receive msg.")
	}

	if sc != 1 {
		t.Errorf("Client not receive response.")
	}
}
