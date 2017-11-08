package modules

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"strings"
)

type AbstractService struct {
	ConfigureInitializer  *ConfigureInitializer
	DefaultMQAdminExtImpl *admin.DefaultMQAdminExtImpl
}

func NewAbstractService() *AbstractService {
	baseService := new(AbstractService)
	baseService.ConfigureInitializer = NewConfigureInitializer()
	return baseService
}

func (service *AbstractService) InitMQAdmin() *admin.DefaultMQAdminExtImpl {
	namesrvAddr := service.ConfigureInitializer.GetNamesrvAddr()
	if strings.TrimSpace(namesrvAddr) == "" {
		panic(fmt.Errorf("please set '%s' environment to blotmq web console", stgcommon.NAMESRV_ADDR_ENV))
	}
	service.DefaultMQAdminExtImpl = admin.NewDefaultMQAdminExtImpl(namesrvAddr)
	return service.DefaultMQAdminExtImpl
}

func (service *AbstractService) Start() {
	if service.DefaultMQAdminExtImpl != nil {
		err := service.DefaultMQAdminExtImpl.Start()
		if err != nil {
			logger.Errorf("DefaultMQAdminExtImpl Start err: %s", err.Error())
			panic(err)
		}
	}
}

func (service *AbstractService) Shutdown() {
	if service.DefaultMQAdminExtImpl != nil {
		err := service.DefaultMQAdminExtImpl.Shutdown()
		if err != nil {
			logger.Errorf("DefaultMQAdminExtImpl Shutdown err: %s", err.Error())
			return
		}
	}
}
