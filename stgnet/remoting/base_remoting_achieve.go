package remoting

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	cmprotocol "git.oschina.net/cloudzone/smartgo/stgcommon/protocol"
	"git.oschina.net/cloudzone/smartgo/stgnet/protocol"
	"github.com/go-errors/errors"
)

type BaseRemotingAchieve struct {
	responseTable           map[int32]*ResponseFuture
	responseTableLock       sync.RWMutex
	rpcHook                 RPCHook
	defaultRequestProcessor RequestProcessor
	processorTable          map[int32]RequestProcessor // 注册的处理器
	processorTableLock      sync.RWMutex
	timeoutTimer            *time.Timer
	isRunning               bool
}

// RegisterProcessor register porcessor
func (ra *BaseRemotingAchieve) RegisterProcessor(requestCode int32, processor RequestProcessor) {
	if ra.processorTable == nil {
		ra.processorTable = make(map[int32]RequestProcessor)
	}

	ra.processorTableLock.Lock()
	ra.processorTable[requestCode] = processor
	ra.processorTableLock.Unlock()
}

// RegisterDefaultProcessor register default porcessor
func (ra *BaseRemotingAchieve) RegisterDefaultProcessor(processor RequestProcessor) {
	ra.defaultRequestProcessor = processor
}

// RegisterRPCHook 注册rpc hook
func (ra *BaseRemotingAchieve) RegisterRPCHook(rpcHook RPCHook) {
	ra.rpcHook = rpcHook
}

func (ra *BaseRemotingAchieve) processReceived(buffer []byte, addr string, conn net.Conn) {
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
	ra.startGoRoutine(func() {
		ra.processMessageReceived(addr, conn, buf)
	})
}

func (ra *BaseRemotingAchieve) processMessageReceived(addr string, conn net.Conn, buf *bytes.Buffer) {
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
		ra.processRequestCommand(addr, conn, remotingCommand)
	case protocol.RESPONSE_COMMAND:
		ra.processResponseCommand(addr, conn, remotingCommand)
	default:
	}
}

func (ra *BaseRemotingAchieve) processRequestCommand(addr string, conn net.Conn, remotingCommand *protocol.RemotingCommand) {
	ra.processorTableLock.Lock()
	processor, ok := ra.processorTable[remotingCommand.Code]
	ra.processorTableLock.Unlock()
	if !ok {
		processor = ra.defaultRequestProcessor
	}

	if processor == nil {
		errMsg := fmt.Sprintf("request type %d not supported", remotingCommand.Code)
		response := protocol.CreateResponseCommand(protocol.REQUEST_CODE_NOT_SUPPORTED, errMsg)
		response.Opaque = remotingCommand.Opaque
		ra.sendResponse(response, addr, conn)
		logger.Fatalf("addr[%s] %s", addr, errMsg)
		return
	}

	// rpc hook before
	if ra.rpcHook != nil {
		ra.rpcHook.DoBeforeRequest(addr, conn, remotingCommand)
	}

	response, err := processor.ProcessRequest(addr, conn, remotingCommand)

	// rpc hook after
	if ra.rpcHook != nil {
		ra.rpcHook.DoAfterResponse(addr, conn, remotingCommand, response)
	}

	if err != nil {
		response := protocol.CreateResponseCommand(protocol.SYSTEM_ERROR, err.Error())
		response.Opaque = remotingCommand.Opaque
		ra.sendResponse(response, addr, conn)
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
	ra.sendResponse(response, addr, conn)
}

func (ra *BaseRemotingAchieve) processResponseCommand(addr string, conn net.Conn, response *protocol.RemotingCommand) {
	// 获取响应
	ra.responseTableLock.RLock()
	responseFuture, ok := ra.responseTable[response.Opaque]
	ra.responseTableLock.RUnlock()
	if !ok {
		if response.Code == cmprotocol.NOTIFY_CONSUMER_IDS_CHANGED {
			// TODO:
		} else {
			logger.Fatalf("processResponseCommand not found responseFuture: %d %v", response.Opaque, response)
		}
		return
	}

	ra.responseTableLock.Lock()
	delete(ra.responseTable, response.Opaque)
	ra.responseTableLock.Unlock()

	responseFuture.responseCommand = response
	if responseFuture.invokeCallback != nil {
		responseFuture.invokeCallback(responseFuture)
	}

	if responseFuture.done != nil {
		responseFuture.done <- true
	}
}

func (ra *BaseRemotingAchieve) sendRequest(request *protocol.RemotingCommand, addr string, conn net.Conn) error {
	return ra.send(request, addr, conn)
}

func (ra *BaseRemotingAchieve) startGoRoutine(fn func()) {
	if ra.isRunning {
		go fn()
	}
}

func (ra *BaseRemotingAchieve) sendResponse(response *protocol.RemotingCommand, addr string, conn net.Conn) error {
	return ra.send(response, addr, conn)
}

func (ra *BaseRemotingAchieve) send(remotingCommand *protocol.RemotingCommand, addr string, conn net.Conn) error {
	header := remotingCommand.EncodeHeader()
	body := remotingCommand.Body

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(len(header)+len(body)+4))
	binary.Write(buf, binary.BigEndian, int32(len(header)))

	//_, err := ra.bootstrap.Write(addr, buf.Bytes())
	_, err := conn.Write(buf.Bytes())
	if err != nil {
		return errors.Wrap(err, 0)
	}

	//_, err = ra.bootstrap.Write(addr, header)
	_, err = conn.Write(header)
	if err != nil {
		return errors.Wrap(err, 0)
	}

	if body != nil && len(body) > 0 {
		//_, err = ra.bootstrap.Write(addr, body)
		_, err = conn.Write(body)
		if err != nil {
			return errors.Wrap(err, 0)
		}
	}

	return nil
}

func (ra *BaseRemotingAchieve) invokeSync(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) (*protocol.RemotingCommand, error) {
	responseFuture := newResponseFuture(request.Opaque, timeoutMillis)
	responseFuture.done = make(chan bool)

	ra.responseTableLock.Lock()
	ra.responseTable[request.Opaque] = responseFuture
	ra.responseTableLock.Unlock()

	err := ra.sendRequest(request, addr, conn)
	if err != nil {
		logger.Fatalf("invokeSync->sendRequest failed: %s %v", addr, err)
		return nil, err
	}
	responseFuture.sendRequestOK = true

	select {
	case <-responseFuture.done:
		return responseFuture.responseCommand, nil
	case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
		return nil, errors.Errorf("invoke sync timeout")
	}
}

func (ra *BaseRemotingAchieve) invokeAsync(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64, invokeCallback InvokeCallback) error {
	responseFuture := newResponseFuture(request.Opaque, timeoutMillis)
	responseFuture.invokeCallback = invokeCallback

	ra.responseTableLock.Lock()
	ra.responseTable[request.Opaque] = responseFuture
	ra.responseTableLock.Unlock()

	err := ra.sendRequest(request, addr, conn)
	if err != nil {
		logger.Fatalf("invokeASync->sendRequest failed: %s %v", addr, err)
		return err
	}
	responseFuture.sendRequestOK = true

	return nil
}

func (ra *BaseRemotingAchieve) invokeOneway(addr string, conn net.Conn, request *protocol.RemotingCommand, timeoutMillis int64) error {
	err := ra.sendRequest(request, addr, conn)
	if err != nil {
		logger.Fatalf("invokeOneway->sendRequest failed: %s %v", addr, err)
		return err
	}

	return nil
}

// 扫描发送请求响应报文是否超时
func (ra *BaseRemotingAchieve) scanResponseTable() {
	ra.responseTableLock.Lock()
	for seq, responseFuture := range ra.responseTable {
		if (responseFuture.beginTimestamp + responseFuture.timeoutMillis + 1000) <= time.Now().Unix()*1000 {
			delete(ra.responseTable, seq)

			if responseFuture.invokeCallback != nil {
				responseFuture.invokeCallback(responseFuture)
				logger.Fatalf("remove time out request %v", responseFuture)
			}
		}
	}
	ra.responseTableLock.Unlock()
}

// 定时扫描响应
func (ra *BaseRemotingAchieve) startScheduledTask() {
	ra.startGoRoutine(func() {
		ra.timeoutTimer = time.NewTimer(3 * time.Second)
		for {
			<-ra.timeoutTimer.C
			ra.scanResponseTable()
			ra.timeoutTimer.Reset(time.Second)
		}
	})
}