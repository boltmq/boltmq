package admin

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/help"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
)

// DefaultMQAdminExtImpl 所有运维接口都在这里实现
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/2
type DefaultMQAdminExtImpl struct {
	createTopicKey   string
	adminExtGroup    string
	ServiceState     stgcommon.ServiceState
	MqClientInstance *process.MQClientInstance
	RpcHook          remoting.RPCHook
	ClientConfig     *stgclient.ClientConfig
	MQAdminExtInner
}

// NewDefaultMQAdminExtImpl 初始化admin控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func NewDefaultMQAdminExtImpl() *DefaultMQAdminExtImpl {
	defaultMQAdminExtImpl := &DefaultMQAdminExtImpl{}
	defaultMQAdminExtImpl.adminExtGroup = "admin_ext_group"
	defaultMQAdminExtImpl.createTopicKey = stgcommon.DEFAULT_TOPIC
	defaultMQAdminExtImpl.ServiceState = stgcommon.CREATE_JUST
	defaultMQAdminExtImpl.ClientConfig = stgclient.NewClientConfig("")
	return defaultMQAdminExtImpl
}

// NewCustomMQAdminExtImpl 初始化admin控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func NewCustomMQAdminExtImpl(rpcHook remoting.RPCHook) *DefaultMQAdminExtImpl {
	defaultMQAdminExtImpl := NewDefaultMQAdminExtImpl()
	defaultMQAdminExtImpl.RpcHook = rpcHook
	return defaultMQAdminExtImpl
}

// GetCreateTopicKey 查询创建Topic的key值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *DefaultMQAdminExtImpl) GetCreateTopicKey() string {
	return impl.createTopicKey
}

// GetAdminExtGroup 查询admin管理的组名称
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *DefaultMQAdminExtImpl) GetAdminExtGroup() string {
	return impl.adminExtGroup
}

// Start 启动Admin
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *DefaultMQAdminExtImpl) Start() error {
	switch impl.ServiceState {
	case stgcommon.CREATE_JUST:

		adminExtGroup := impl.GetAdminExtGroup()
		impl.ServiceState = stgcommon.START_FAILED
		impl.ClientConfig.ChangeInstanceNameToPID()
		impl.MqClientInstance = process.GetInstance().GetAndCreateMQClientInstance(impl.ClientConfig)

		registerOK := impl.registerAdminExt(adminExtGroup, impl)
		if !registerOK {
			impl.ServiceState = stgcommon.CREATE_JUST
			format := "the adminExt group[%s] has created already, specifed another name please. %s"
			return fmt.Errorf(format, adminExtGroup, help.SuggestTodo(help.GROUP_NAME_DUPLICATE_URL))
		}
		impl.MqClientInstance.Start()
		logger.Infof("the adminExt [%s] start OK", adminExtGroup)
		impl.ServiceState = stgcommon.RUNNING
	case stgcommon.RUNNING:
	case stgcommon.START_FAILED:
	case stgcommon.SHUTDOWN_ALREADY:
		format := "the AdminExt service state not OK, maybe started once. serviceState=%d(%s), %s"
		return fmt.Errorf(format, int(impl.ServiceState), impl.ServiceState.String(), help.SuggestTodo(help.CLIENT_SERVICE_NOT_OK))
	}
	return nil
}

// Shutdown 关闭Admin
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *DefaultMQAdminExtImpl) Shutdown() error {
	if impl == nil || impl.MqClientInstance == nil {
		return nil
	}
	switch impl.ServiceState {
	case stgcommon.CREATE_JUST:
	case stgcommon.RUNNING:
		adminExtGroupId := impl.GetAdminExtGroup()
		ok, err := impl.unRegisterAdminExt(adminExtGroupId)
		if err == nil && ok {
			logger.Infof("DefaultMQAdminExtImpl UnRegisterAdminExt successful")
		} else {
			logger.Infof("DefaultMQAdminExtImpl UnRegisterAdminExt failed")
		}

		impl.MqClientInstance.Shutdown()
		logger.Infof("the adminExt [%s] shutdown OK", adminExtGroupId)
		impl.ServiceState = stgcommon.SHUTDOWN_ALREADY
	case stgcommon.SHUTDOWN_ALREADY:
	default:
	}
	return nil
}

// registerAdminExt 注册Admin控制对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *DefaultMQAdminExtImpl) registerAdminExt(groupId string, mqAdminExtInner MQAdminExtInner) bool {
	if groupId == "" || mqAdminExtInner == nil {
		return false
	}
	prev, err := impl.MqClientInstance.AdminExtTable.PutIfAbsent(groupId, mqAdminExtInner)
	if err != nil {
		logger.Errorf("DefaultMQAdminExtImpl RegisterAdminExt err: %s, groupId: %s", err.Error(), groupId)
		return false
	}
	if prev != nil {
		logger.Warnf("the admin groupId[%s] exist already.", groupId)
		return false
	}
	return true
}

// unRegisterAdminExt 卸载Admin控制对象
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *DefaultMQAdminExtImpl) unRegisterAdminExt(groupId string) (bool, error) {
	if groupId == "" || impl.MqClientInstance == nil || impl.MqClientInstance.AdminExtTable == nil {
		return false, nil
	}

	prev, err := impl.MqClientInstance.AdminExtTable.Remove(groupId)
	if err != nil {
		logger.Errorf("AdminExtTable unRegisterAdminExt failed. groupId: %s, err: %s", groupId, err.Error())
		return false, err
	}
	if prev != nil {
		if mqAdminExtInner, ok := prev.(MQAdminExtInner); ok && mqAdminExtInner != nil {
			return true, nil
		} else {
			logger.Warnf("AdminExtTable unRegisterAdminExt failed. groupId: %s", groupId)
			return false, nil
		}
	}
	return false, nil
}
