package stgstorelog

import (
	"net"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

// AcceptSocketService
// Author zhoufei
// Since 2017/10/19
type AcceptSocketService struct {
	listener  *net.TCPListener
	haService *HAService
	port      int32
	stoped    bool
}

func NewAcceptSocketService(port int32, haService *HAService) *AcceptSocketService {
	service := new(AcceptSocketService)
	service.haService = haService
	service.port = port
	service.stoped = false

	serverAddress := fmt.Sprintf(":%d", port)
	serverAddr, err := net.ResolveTCPAddr("tcp", serverAddress)
	if err != nil {
		logger.Error("accept socket service resolve server address error:", err.Error())
		return nil
	}

	listener, err := net.ListenTCP("tcp", serverAddr)
	if err != nil {
		logger.Error("accept socket service listener port error:", err.Error())
		return nil
	}

	service.listener = listener
	return service
}

func (self *AcceptSocketService) start() {
	logger.Info("accept socket service started")

	for {
		if self.stoped {
			break
		}

		connection, err := self.listener.AcceptTCP()
		if err != nil {
			logger.Error("accept socket service accept error:", err.Error())
			continue
		}

		logger.Info("HAService receive new connection, ", connection.RemoteAddr().String())
		haConnection := NewHAConnection(self.haService, connection)

		go func() {
			haConnection.start()
		}()

		self.haService.addConnection(haConnection)
	}

	self.listener.Close()
	logger.Info("accept socket service end")
}

func (self *AcceptSocketService) Shutdown(interrupt bool) {
	self.stoped = true
}
