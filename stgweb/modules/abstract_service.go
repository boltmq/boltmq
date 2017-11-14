package modules

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient/admin"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"strings"
	"sync"
)

var (
	baseService *AbstractService
	sOnce       sync.Once
)

type AbstractService struct {
	ConfigureInitializer *ConfigureInitializer
}

func Default() *AbstractService {
	sOnce.Do(func() {
		baseService = newAbstractService()
	})
	return baseService
}

func newAbstractService() *AbstractService {
	abstractService := new(AbstractService)
	abstractService.ConfigureInitializer = newConfigureInitializer()
	return abstractService
}

func (service *AbstractService) GetDefaultMQAdminExtImpl() *admin.DefaultMQAdminExtImpl {
	namesrvAddr := service.ConfigureInitializer.GetNamesrvAddr()
	if strings.TrimSpace(namesrvAddr) == "" {
		panic(fmt.Errorf("please set '%s' environment to blotmq console", stgcommon.NAMESRV_ADDR_ENV))
	}
	defaultMQAdminExt := admin.NewDefaultMQAdminExtImpl(namesrvAddr)
	return defaultMQAdminExt
}
