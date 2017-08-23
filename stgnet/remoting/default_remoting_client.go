package remoting

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// DefalutRemotingClient default remoting client
type DefalutRemotingClient struct {
	bootstrap           *netm.Bootstrap
	namesrvAddrList     []string
	namesrvAddrListLock sync.RWMutex
	namesrvAddrChoosed  string
	namesrvIndex        uint32
	timeoutTimer        *time.Timer
	isRunning           bool
	BaseRemotingClient
}

// NewDefalutRemotingClient return new default remoting client
func NewDefalutRemotingClient() *DefalutRemotingClient {
	remotingClient := &DefalutRemotingClient{}
	remotingClient.responseTable = make(map[int32]*ResponseFuture)
	remotingClient.bootstrap = netm.NewBootstrap()
	return remotingClient
}

// Start start client
func (rc *DefalutRemotingClient) Start() {
	rc.bootstrap.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		// 开启gorouting处理响应
		rc.startGoRoutine(func() {
			rc.processReceived(buffer, addr, conn)
		})
	})

	// 定时扫描响应
	go func() {
		rc.timeoutTimer = time.NewTimer(3 * time.Second)
		for {
			<-rc.timeoutTimer.C
			rc.scanResponseTable()
			rc.timeoutTimer.Reset(time.Second)
		}
	}()

	rc.isRunning = true
}

// Shutdown shutdown client
func (rc *DefalutRemotingClient) Shutdown() {
	rc.timeoutTimer.Stop()
	rc.bootstrap.Shutdown()
	rc.isRunning = false
}

// GetNameServerAddressList return nameserver addr list
func (rc *DefalutRemotingClient) GetNameServerAddressList() []string {
	rc.namesrvAddrListLock.RLock()
	defer rc.namesrvAddrListLock.RUnlock()
	return rc.namesrvAddrList
}

// UpdateNameServerAddressList update nameserver addrs list
func (rc *DefalutRemotingClient) UpdateNameServerAddressList(addrs []string) {
	var (
		repeat bool
	)

	rc.namesrvAddrListLock.Lock()
	for _, addr := range addrs {
		// 去除重复地址
		for _, oaddr := range rc.namesrvAddrList {
			if addr == oaddr {
				repeat = true
				break
			}
		}

		if repeat == false {
			rc.namesrvAddrList = append(rc.namesrvAddrList, addr)
		}
	}
	rc.namesrvAddrListLock.Unlock()
}

// InvokeSync 同步调用并返回响应, addr为空字符串，则在namesrvAddrList中选择地址
func (rc *DefalutRemotingClient) InvokeSync(addr string, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	conn, err := rc.createConnectByAddr(addr)
	if err != nil {
		return nil, err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, conn, request)
	}

	response, err := rc.invokeSync(addr, conn, request, timeoutMillis)

	// rpc hook after
	if rc.rpcHook != nil {
		rc.rpcHook.DoAfterResponse(addr, conn, request, response)
	}

	return response, err
}

func (rc *DefalutRemotingClient) invokeSync(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	responseFuture := newResponseFuture(request.Opaque, timeoutMillis)
	responseFuture.done = make(chan bool)

	rc.responseTableLock.Lock()
	rc.responseTable[request.Opaque] = responseFuture
	rc.responseTableLock.Unlock()

	err := rc.sendRequest(request, addr, conn)
	if err != nil {
		logger.Fatalf("invokeSync->sendRequest failed: %s %v", addr, err)
		return nil, err
	}
	responseFuture.sendRequestOK = true

	select {
	case <-responseFuture.done:
		return responseFuture.responseCommand, nil
	case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
		return nil, errors.New("invoke sync timeout")
	}
}

// InvokeAsync 异步调用
func (rc *DefalutRemotingClient) InvokeAsync(addr string, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	conn, err := rc.createConnectByAddr(addr)
	if err != nil {
		return err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, conn, request)
	}

	return rc.invokeAsync(addr, conn, request, timeoutMillis, invokeCallback)
}

func (rc *DefalutRemotingClient) invokeAsync(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	responseFuture := newResponseFuture(request.Opaque, timeoutMillis)
	responseFuture.invokeCallback = invokeCallback

	rc.responseTableLock.Lock()
	rc.responseTable[request.Opaque] = responseFuture
	rc.responseTableLock.Unlock()

	err := rc.sendRequest(request, addr, conn)
	if err != nil {
		logger.Fatalf("invokeASync->sendRequest failed: %s %v", addr, err)
		return err
	}
	responseFuture.sendRequestOK = true

	return nil
}

// InvokeSync 单向发送消息
func (rc *DefalutRemotingClient) InvokeOneway(addr string, request *protocol.RemotingCommand, timeoutMillis int64) error {
	conn, err := rc.createConnectByAddr(addr)
	if err != nil {
		return err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, conn, request)
	}

	return rc.invokeOneway(addr, conn, request, timeoutMillis)
}

func (rc *DefalutRemotingClient) invokeOneway(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) error {
	err := rc.sendRequest(request, addr, conn)
	if err != nil {
		logger.Fatalf("invokeOneway->sendRequest failed: %s %v", addr, err)
		return err
	}

	return nil
}

// 扫描发送请求响应报文是否超时
func (rc *DefalutRemotingClient) scanResponseTable() {
	rc.responseTableLock.Lock()
	for seq, responseFuture := range rc.responseTable {
		if (responseFuture.beginTimestamp + responseFuture.timeoutMillis + 1000) <= time.Now().Unix()*1000 {
			delete(rc.responseTable, seq)

			if responseFuture.invokeCallback != nil {
				responseFuture.invokeCallback(responseFuture)
				logger.Fatalf("remove time out request %v", responseFuture)
			}
		}
	}
	rc.responseTableLock.Unlock()
}

func (rc *DefalutRemotingClient) createConnectByAddr(addr string) (net.Conn, error) {
	if addr == "" {
		addr = rc.chooseNameseverAddr()
	}

	// 创建连接，如果连接存在，则不会创建。
	return rc.bootstrap.ConnectJoinAddrAndReturn(addr)
}

func (rc *DefalutRemotingClient) chooseNameseverAddr() string {
	if rc.namesrvAddrChoosed != "" {
		//return rc.namesrvAddrChoosed
		// 判断连接是否可用，不可用选取其它name server addr
		if rc.bootstrap.HasConnect(rc.namesrvAddrChoosed) {
			return rc.namesrvAddrChoosed
		}
	}

	var (
		caddr string
		nlen  uint32
		i     uint32
	)
	rc.namesrvAddrListLock.RLock()
	nlen = uint32(len(rc.namesrvAddrList))
	for ; i < nlen; i++ {
		atomic.AddUint32(&rc.namesrvIndex, 1)
		idx := rc.namesrvIndex % nlen

		newAddr := rc.namesrvAddrList[idx]
		rc.namesrvAddrChoosed = newAddr
		//caddr = rc.namesrvAddrChoosed
		//break
		if rc.bootstrap.HasConnect(newAddr) {
			caddr = rc.namesrvAddrChoosed
			break
		}
	}
	rc.namesrvAddrListLock.RUnlock()

	return caddr
}

func (rc *DefalutRemotingClient) sendRequest(request *protocol.RemotingCommand, addr string, conn net.Conn) error {
	return rc.send(request, addr, conn)
}

func (rc *DefalutRemotingClient) startGoRoutine(fn func()) {
	if rc.isRunning {
		go fn()
	}
}

// RegisterProcessor register porcessor
func (rc *DefalutRemotingClient) RegisterProcessor(requestCode int, processor RequestProcessor) {
	rc.processorTableLock.Lock()
	rc.processorTable[requestCode] = processor
	rc.processorTableLock.Unlock()
}
