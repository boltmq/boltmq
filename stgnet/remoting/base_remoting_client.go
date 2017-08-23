package remoting

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	cmprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
)

type BaseRemotingClient struct {
	responseTable           map[int32]*ResponseFuture
	responseTableLock       sync.RWMutex
	rpcHook                 RPCHook
	defaultRequestProcessor RequestProcessor
	processorTable          map[int]RequestProcessor // 注册的处理器
	processorTableLock      sync.RWMutex
	isRunning               bool
}

// RegisterProcessor register porcessor
func (rc *BaseRemotingClient) RegisterProcessor(requestCode int, processor RequestProcessor) {
	rc.processorTableLock.Lock()
	rc.processorTable[requestCode] = processor
	rc.processorTableLock.Unlock()
}

// RegisterRPCHook 注册rpc hook
func (rc *BaseRemotingClient) RegisterRPCHook(rpcHook RPCHook) {
	rc.rpcHook = rpcHook
}

func (rc *BaseRemotingClient) processReceived(buffer []byte, addr string, conn net.Conn) {
	var (
		buf = bytes.NewBuffer([]byte{})
	)

	// 安全考虑进行拷贝数据，之后使用队列缓存
	_, err := buf.Write(buffer)
	if err != nil {
		logger.Fatalf("processReceived write buffer failed: %v", err)
		return
	}

	// 开启gorouting处理响应
	rc.startGoRoutine(func() {
		rc.processMessageReceived(addr, conn, buf)
	})
}

func (rc *BaseRemotingClient) processMessageReceived(addr string, conn net.Conn, buf *bytes.Buffer) {
	// 解析报文
	remotingCommand, err := protocol.DecodeRemotingCommand(buf)
	if err != nil {
		logger.Fatalf("processReceived deconde failed: %v", err)
		return
	}

	if remotingCommand == nil {
		return
	}

	switch remotingCommand.Type() {
	case protocol.REQUEST_COMMAND:
		rc.processRequestCommand(addr, conn, remotingCommand)
	case protocol.RESPONSE_COMMAND:
		rc.processResponseCommand(addr, conn, remotingCommand)
	default:
	}
}

func (rc *BaseRemotingClient) processRequestCommand(addr string, conn net.Conn, remotingCommand *protocol.RemotingCommand) {
	rc.processorTableLock.Lock()
	processor, ok := rc.processorTable[remotingCommand.Code]
	rc.processorTableLock.Unlock()
	if !ok {
		processor = rc.defaultRequestProcessor
	}

	if processor == nil {
		errMsg := fmt.Sprintf("request type %d not supported", remotingCommand.Code)
		response := protocol.CreateResponseCommand(protocol.REQUEST_CODE_NOT_SUPPORTED, errMsg)
		response.Opaque = remotingCommand.Opaque
		rc.sendResponse(response, addr, conn)
		logger.Fatalf("addr[%s] %s", addr, errMsg)
		return
	}

	// rpc hook before
	if rc.rpcHook != nil {
		rc.rpcHook.DoBeforeRequest(addr, conn, remotingCommand)
	}

	response, err := processor.ProcessRequest(addr, conn, remotingCommand)

	// rpc hook after
	if rc.rpcHook != nil {
		rc.rpcHook.DoAfterResponse(addr, conn, remotingCommand, response)
	}

	if err != nil {
		response := protocol.CreateResponseCommand(protocol.SYSTEM_ERROR, err.Error())
		response.Opaque = remotingCommand.Opaque
		rc.sendResponse(response, addr, conn)
		logger.Fatalf("process request exception %v", err)
		return
	}

	if remotingCommand.IsOnewayRPC() {
		return
	}

	if response == nil {
		// 收到请求，但是没有返回应答，可能是processRequest中进行了应答，忽略这种情况
		return
	}

	response.Opaque = remotingCommand.Opaque
	response.MarkResponseType()
	rc.sendResponse(response, addr, conn)
}

func (rc *BaseRemotingClient) processResponseCommand(addr string, conn net.Conn, response *protocol.RemotingCommand) {
	// 获取响应
	rc.responseTableLock.RLock()
	responseFuture, ok := rc.responseTable[response.Opaque]
	rc.responseTableLock.RUnlock()
	if !ok {
		if response.Code == cmprotocol.NOTIFY_CONSUMER_IDS_CHANGED {
			// TODO:
		} else {
			logger.Fatalf("processResponseCommand not found responseFuture: %d %v", response.Opaque, response)
		}
		return
	}

	rc.responseTableLock.Lock()
	delete(rc.responseTable, response.Opaque)
	rc.responseTableLock.Unlock()

	responseFuture.responseCommand = response
	if responseFuture.invokeCallback != nil {
		responseFuture.invokeCallback(responseFuture)
	}

	if responseFuture.done != nil {
		responseFuture.done <- true
	}
}

func (rc *BaseRemotingClient) sendRequest(request *protocol.RemotingCommand, addr string, conn net.Conn) error {
	return rc.send(request, addr, conn)
}

func (rc *BaseRemotingClient) startGoRoutine(fn func()) {
	if rc.isRunning {
		go fn()
	}
}

func (rc *BaseRemotingClient) sendResponse(response *protocol.RemotingCommand, addr string, conn net.Conn) error {
	return rc.send(response, addr, conn)
}

func (rc *BaseRemotingClient) send(remotingCommand *protocol.RemotingCommand, addr string, conn net.Conn) error {
	header := remotingCommand.EncodeHeader()
	body := remotingCommand.Body

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(len(header)+len(body)+4))
	binary.Write(buf, binary.BigEndian, int32(len(header)))

	//_, err := rc.bootstrap.Write(addr, buf.Bytes())
	_, err := conn.Write(buf.Bytes())
	if err != nil {
		return err
	}

	//_, err = rc.bootstrap.Write(addr, header)
	_, err = conn.Write(header)
	if err != nil {
		return err
	}

	if body != nil && len(body) > 0 {
		//_, err = rc.bootstrap.Write(addr, body)
		_, err = conn.Write(body)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rc *BaseRemotingClient) invokeSync(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
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

func (rc *BaseRemotingClient) invokeAsync(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
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

func (rc *BaseRemotingClient) invokeOneway(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) error {
	err := rc.sendRequest(request, addr, conn)
	if err != nil {
		logger.Fatalf("invokeOneway->sendRequest failed: %s %v", addr, err)
		return err
	}

	return nil
}

// 扫描发送请求响应报文是否超时
func (rc *BaseRemotingClient) scanResponseTable() {
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
