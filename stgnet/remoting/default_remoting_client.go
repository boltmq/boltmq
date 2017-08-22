package remoting

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgnet/netm"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

// DefalutRemotingClient default remoting client
type DefalutRemotingClient struct {
	bootstrap          *netm.Bootstrap
	responseTable      map[int32]*ResponseFuture
	responseTableLock  sync.RWMutex
	rpcHook            RPCHook
	namesrvAddrList    []string
	namesrvAddrChoosed string
	isClosed           bool
}

// NewDefalutRemotingClient return new default remoting client
func NewDefalutRemotingClient() *DefalutRemotingClient {
	remotingClient := &DefalutRemotingClient{
		responseTable: make(map[int32]*ResponseFuture),
	}

	remotingClient.bootstrap = netm.NewBootstrap()
	return remotingClient
}

// Start start client
func (rc *DefalutRemotingClient) Start() {
	rc.bootstrap.RegisterHandler(func(buffer []byte, addr string, conn net.Conn) {
		fmt.Println("rece:", string(buffer))
	})
}

// Shutdown shutdown client
func (rc *DefalutRemotingClient) Shutdown() {
	rc.bootstrap.Shutdown()
}

// GetNameServerAddressList return nameserver addr list
func (rc *DefalutRemotingClient) GetNameServerAddressList() []string {
	return rc.namesrvAddrList
}

// InvokeSync 同步调用并返回响应
func (rc *DefalutRemotingClient) InvokeSync(addr string, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	// 创建连接，如果连接存在，则不会创建。
	err := rc.bootstrap.ConnectJoinAddr(addr)
	if err != nil {
		return nil, err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, request)
	}

	response, err := rc.invokeSync(addr, request, timeoutMillis)

	// rpc hook after
	if rc.rpcHook != nil {
		rc.rpcHook.DoAfterResponse(addr, request, response)
	}

	return response, err
}

func (rc *DefalutRemotingClient) invokeSync(addr string, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	response := newResponseFuture(request.Opaque, timeoutMillis)
	header := request.EncodeHeader()

	rc.responseTableLock.Lock()
	rc.responseTable[request.Opaque] = response
	rc.responseTableLock.Unlock()

	err := rc.sendRequest(header, request.Body, addr)
	if err != nil {
		rc.bootstrap.Fatalf("invokeSync->sendRequest failed: %s %v", addr, err)
		return nil, err
	}
	response.sendRequestOK = true

	select {
	case <-response.done:
		return response.responseCommand, nil
	case <-time.After(3 * time.Second):
		return nil, errors.New("invoke sync timeout")
	}
}

// InvokeAsync 异步调用
func (rc *DefalutRemotingClient) InvokeAsync(addr string, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	// 创建连接，如果连接存在，则不会创建。
	err := rc.bootstrap.ConnectJoinAddr(addr)
	if err != nil {
		return err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, request)
	}

	return rc.invokeAsync(addr, request, timeoutMillis, invokeCallback)
}

func (rc *DefalutRemotingClient) invokeAsync(addr string, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	response := newResponseFuture(request.Opaque, timeoutMillis)
	response.invokeCallback = invokeCallback
	header := request.EncodeHeader()

	rc.responseTableLock.Lock()
	rc.responseTable[request.Opaque] = response
	rc.responseTableLock.Unlock()

	err := rc.sendRequest(header, request.Body, addr)
	if err != nil {
		rc.bootstrap.Fatalf("invokeASync->sendRequest failed: %s %v", addr, err)
		return err
	}
	response.sendRequestOK = true

	return nil
}

// InvokeSync 单向发送消息
func (rc *DefalutRemotingClient) InvokeOneway(addr string, request *protocol.RemotingCommand, timeoutMillis int64) error {
	// 创建连接，如果连接存在，则不会创建。
	err := rc.bootstrap.ConnectJoinAddr(addr)
	if err != nil {
		return err
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, request)
	}

	return rc.invokeOneway(addr, request, timeoutMillis)
}

func (rc *DefalutRemotingClient) invokeOneway(addr string, request *protocol.RemotingCommand, timeoutMillis int64) error {
	header := request.EncodeHeader()

	err := rc.sendRequest(header, request.Body, addr)
	if err != nil {
		rc.bootstrap.Fatalf("invokeASync->sendRequest failed: %s %v", addr, err)
		return err
	}

	return nil
}

/*
func (rc *DefalutRemotingClient) RegisterProcessor(requestCode int, processor RequestProcessor) {}

*/

func (rc *DefalutRemotingClient) sendRequest(header, body []byte, addr string) error {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(len(header)+len(body)+4))
	binary.Write(buf, binary.BigEndian, int32(len(header)))

	_, err := rc.bootstrap.Write(addr, buf.Bytes())
	if err != nil {
		return err
	}

	_, err = rc.bootstrap.Write(addr, header)
	if err != nil {
		return err
	}

	if body != nil && len(body) > 0 {
		_, err = rc.bootstrap.Write(addr, body)
		if err != nil {
			return err
		}
	}

	return nil
}

// RegisterRPCHook 注册rpc hook
func (rc *DefalutRemotingClient) RegisterRPCHook(rpcHook RPCHook) {
	rc.rpcHook = rpcHook
}
