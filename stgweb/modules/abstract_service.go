package modules

import (
	"git.oschina.net/cloudzone/smartgo/stgclient/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"strconv"
)

type AbstractService struct {
	ConfigureInitializer ConfigureInitializer
}

func NewAbstractService() *AbstractService {
	baseService := new(AbstractService)
	baseService.ConfigureInitializer = NewConfigureInitializer()
	return baseService
}

func (service *AbstractService) GetDefaultMQAdminExt() *admin.DefaultMQAdminExt {
	defaultMQAdminExt := admin.NewDefaultMQAdminExt()
	defaultMQAdminExt.ClientConfig.InstanceName = strconv.FormatInt(timeutil.NowTimestamp(), 10)
	return defaultMQAdminExt
}

func (service *AbstractService) Start(defaultMQAdminExt *admin.DefaultMQAdminExt) {
	if defaultMQAdminExt != nil {
		err := defaultMQAdminExt.Start()
		if err != nil {
			logger.Errorf("DefaultMQAdminExt Start err: %s", err.Error())
			return
		}
	}
}

func (service *AbstractService) Shutdown(defaultMQAdminExt *admin.DefaultMQAdminExt) {
	if defaultMQAdminExt != nil {
		err := defaultMQAdminExt.Shutdown()
		if err != nil {
			logger.Errorf("DefaultMQAdminExt Shutdown err: %s", err.Error())
			return
		}
	}
}
