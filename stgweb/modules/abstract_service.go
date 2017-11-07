package modules

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
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
	service.DefaultMQAdminExtImpl = admin.NewDefaultMQAdminExtImpl()
	return service.DefaultMQAdminExtImpl
}

func (service *AbstractService) Start() {
	if service.DefaultMQAdminExtImpl != nil {
		err := service.DefaultMQAdminExtImpl.Start()
		if err != nil {
			logger.Errorf("DefaultMQAdminExtImpl Start err: %s", err.Error())
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
