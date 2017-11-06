package modules

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"strconv"
)

type AbstractService struct {
	ConfigureInitializer *ConfigureInitializer
	DefaultMQAdminExt    *admin.DefaultMQAdminExtImpl
}

func NewAbstractService() *AbstractService {
	baseService := new(AbstractService)
	baseService.ConfigureInitializer = NewConfigureInitializer()
	return baseService
}

func (service *AbstractService) BuildDefaultMQAdminExt() *admin.DefaultMQAdminExtImpl {
	service.DefaultMQAdminExt = admin.NewDefaultMQAdminExtImpl()
	service.DefaultMQAdminExt.ClientConfig.InstanceName = strconv.FormatInt(timeutil.NowTimestamp(), 10)
	return service.DefaultMQAdminExt
}

func (service *AbstractService) Start() {
	if service.DefaultMQAdminExt != nil {
		err := service.DefaultMQAdminExt.Start()
		if err != nil {
			logger.Errorf("DefaultMQAdminExt Start err: %s", err.Error())
		}
	}
}

func (service *AbstractService) Shutdown() {
	if service.DefaultMQAdminExt != nil {
		err := service.DefaultMQAdminExt.Shutdown()
		if err != nil {
			logger.Errorf("DefaultMQAdminExt Shutdown err: %s", err.Error())
			return
		}
	}
}
