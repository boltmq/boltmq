package admin

import (
	"fmt"
	//"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/help"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
)

// DefaultMQAdminExt 所有运维接口都在这里实现
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
type DefaultMQAdminExt struct {
	CreateTopicKey string
	AdminExtGroup  string
	DefaultMQAdminExtImpl
}

// NewDefaultMQAdminExt 初始化admin控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func NewDefaultMQAdminExt() *DefaultMQAdminExt {
	defaultMQAdminExt := &DefaultMQAdminExt{
		AdminExtGroup:  "admin_ext_group",
		CreateTopicKey: stgcommon.DEFAULT_TOPIC,
	}
	return defaultMQAdminExt
}

// GetCreateTopicKey 查询创建Topic的key值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl DefaultMQAdminExt) GetCreateTopicKey() string {
	return impl.CreateTopicKey
}

// GetAdminExtGroup 查询admin管理的组名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl DefaultMQAdminExt) GetAdminExtGroup() string {
	return impl.CreateTopicKey
}
//
//// Start 启动Admin
//// Author: tianyuliang, <tianyuliang@gome.com.cn>
//// Since: 2017/11/6
//func (impl *DefaultMQAdminExt) Start() error {
//	switch impl.ServiceState {
//	case stgcommon.CREATE_JUST:
//
//		adminExtGroup := impl.GetAdminExtGroup()
//		impl.ServiceState = stgcommon.START_FAILED
//		impl.ClientConfig.ChangeInstanceNameToPID()
//		impl.MqClientInstance = process.GetInstance().GetAndCreateMQClientInstance(impl.ClientConfig)
//
//		registerOK := impl.MqClientInstance.RegisterAdminExt(adminExtGroup, impl)
//		if !registerOK {
//			impl.ServiceState = stgcommon.CREATE_JUST
//			format := "the adminExt group[%s] has created already, specifed another name please. %s"
//			return fmt.Errorf(format, adminExtGroup, help.SuggestTodo(help.GROUP_NAME_DUPLICATE_URL))
//		}
//		impl.MqClientInstance.Start()
//		logger.Infof("the adminExt [%s] start OK", adminExtGroup)
//		impl.ServiceState = stgcommon.RUNNING
//	case stgcommon.RUNNING:
//	case stgcommon.START_FAILED:
//	case stgcommon.SHUTDOWN_ALREADY:
//		format := "the AdminExt service state not OK, maybe started once. serviceState=%d(%s), %s"
//		return fmt.Errorf(format, int(impl.ServiceState), impl.ServiceState.String(), help.SuggestTodo(help.CLIENT_SERVICE_NOT_OK))
//	}
//}
