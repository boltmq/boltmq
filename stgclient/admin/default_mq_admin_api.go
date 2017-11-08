package admin

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgclient"
	"git.oschina.net/cloudzone/smartgo/stgclient/process"
	"git.oschina.net/cloudzone/smartgo/stgcommon"
	"git.oschina.net/cloudzone/smartgo/stgcommon/help"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	"git.oschina.net/cloudzone/smartgo/stgnet/remoting"
	"strconv"
	"strings"
)

// DefaultMQAdminExtImpl 所有运维接口都在这里实现
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/2
type DefaultMQAdminExtImpl struct {
	createTopicKey   string
	adminExtGroup    string
	serviceState     stgcommon.ServiceState
	mqClientInstance *process.MQClientInstance
	rpcHook          remoting.RPCHook
	clientConfig     *stgclient.ClientConfig
	MQAdminExtInner
}

// NewDefaultMQAdminExtImpl 初始化admin控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func NewDefaultMQAdminExtImpl(namesrvAddr string) *DefaultMQAdminExtImpl {
	defaultMQAdminExtImpl := &DefaultMQAdminExtImpl{}
	defaultMQAdminExtImpl.adminExtGroup = "admin_ext_group"
	defaultMQAdminExtImpl.createTopicKey = stgcommon.DEFAULT_TOPIC
	defaultMQAdminExtImpl.serviceState = stgcommon.CREATE_JUST
	defaultMQAdminExtImpl.clientConfig = stgclient.NewClientConfig(namesrvAddr)
	defaultMQAdminExtImpl.clientConfig.InstanceName = strconv.FormatInt(timeutil.NowTimestamp(), 10)
	return defaultMQAdminExtImpl
}

// NewCustomMQAdminExtImpl 初始化admin控制器
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/1
func NewCustomMQAdminExtImpl(rpcHook remoting.RPCHook, namesrvAddr string) *DefaultMQAdminExtImpl {
	defaultMQAdminExtImpl := NewDefaultMQAdminExtImpl(namesrvAddr)
	defaultMQAdminExtImpl.rpcHook = rpcHook
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

// 检查配置文件
func (impl *DefaultMQAdminExtImpl) checkConfig() {
	adminExtGroup := impl.GetAdminExtGroup()
	err := process.CheckGroup(adminExtGroup)
	if err != nil {
		panic(err)
	}
	if strings.EqualFold(adminExtGroup, stgcommon.DEFAULT_PRODUCER_GROUP) {
		format := "producerGroup can not equal %s, please specify another one."
		panic(fmt.Sprintf(format, stgcommon.DEFAULT_PRODUCER_GROUP))
	}
}

// Start 启动Admin
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *DefaultMQAdminExtImpl) Start() error {
	switch impl.serviceState {
	case stgcommon.CREATE_JUST:
		// 检查配置
		adminExtGroup := impl.GetAdminExtGroup()
		impl.serviceState = stgcommon.START_FAILED
		impl.checkConfig()
		if !strings.EqualFold(adminExtGroup, stgcommon.CLIENT_INNER_PRODUCER_GROUP) {
			impl.clientConfig.ChangeInstanceNameToPID()
		}

		// 初始化MQClientInstance
		impl.mqClientInstance = process.GetInstance().GetAndCreateMQClientInstance(impl.clientConfig)

		// 注册admin管理控制器
		registerOK := impl.registerAdminExt(adminExtGroup, impl)
		if !registerOK {
			impl.serviceState = stgcommon.CREATE_JUST
			format := "the adminExt group[%s] has created already, specifed another name please. %s"
			return fmt.Errorf(format, adminExtGroup, help.SuggestTodo(help.GROUP_NAME_DUPLICATE_URL))
		}

		// 启动admin实例
		impl.mqClientInstance.ServiceState = stgcommon.START_FAILED
		impl.mqClientInstance.MQClientAPIImpl.Start()
		impl.serviceState = stgcommon.RUNNING
		logger.Infof("the adminExt [%s] start OK", adminExtGroup)
	case stgcommon.RUNNING:
	case stgcommon.START_FAILED:
	case stgcommon.SHUTDOWN_ALREADY:
		format := "the AdminExt service state not OK, maybe started once. serviceState=%d(%s), %s"
		return fmt.Errorf(format, int(impl.serviceState), impl.serviceState.String(), help.SuggestTodo(help.CLIENT_SERVICE_NOT_OK))
	}
	return nil
}

// Shutdown 关闭Admin
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/11/6
func (impl *DefaultMQAdminExtImpl) Shutdown() error {
	if impl == nil || impl.mqClientInstance == nil {
		return nil
	}
	switch impl.serviceState {
	case stgcommon.CREATE_JUST:
	case stgcommon.RUNNING:
		adminExtGroupId := impl.GetAdminExtGroup()
		impl.unRegisterAdminExt(adminExtGroupId)
		impl.mqClientInstance.MQClientAPIImpl.Shutdwon()
		process.GetInstance().RemoveClientFactory(impl.mqClientInstance.ClientId)
		impl.mqClientInstance.ServiceState = stgcommon.SHUTDOWN_ALREADY
		logger.Infof("the adminExt [%s] shutdown OK", adminExtGroupId)
		impl.serviceState = stgcommon.SHUTDOWN_ALREADY
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
	prev, err := impl.mqClientInstance.AdminExtTable.PutIfAbsent(groupId, mqAdminExtInner)
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
	if groupId == "" || impl.mqClientInstance == nil || impl.mqClientInstance.AdminExtTable == nil {
		return false, nil
	}

	prev, err := impl.mqClientInstance.AdminExtTable.Remove(groupId)
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
