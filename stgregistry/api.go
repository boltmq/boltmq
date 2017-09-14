package stgregistry

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/parseutil"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"log"
	"strconv"
	"strings"
	"time"
)

type SmartgoConfig struct {
	Global  globalConfig
	Namesrv map[string]namesrvCfg
}

type globalConfig struct {
	Env     string
	Stghome string
}

type namesrvCfg struct {
	Addrs []string `toml:"etcdaddrs"`
}

func etcdAddrsFromConfig() []string {
	namesrvConfPath := "../conf/smartgo.toml"
	var smartgoConfig SmartgoConfig
	parseutil.ParseConf(namesrvConfPath, &smartgoConfig)

	var etcdAddrs []string
	for key, val := range smartgoConfig.Namesrv {
		if key == smartgoConfig.Global.Env {
			etcdAddrs = val.Addrs
		}
	}
	return etcdAddrs
}

type EtcdApi struct {
	KApi client.KeysAPI
	MApi client.MembersAPI
}

func NewEtcdApi() (etcdApi EtcdApi) {
	etcdAddrs := etcdAddrsFromConfig()

	cfg := client.Config{
		Endpoints: etcdAddrs,
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	etcdApi.KApi = client.NewKeysAPI(c)
	etcdApi.MApi = client.NewMembersAPI(c)
	return etcdApi
}

type Broker struct {
	clusterName string
	brokerAddr  string
	brokerName  string
	brokerId    int64
}

func (self *EtcdApi) RegisterBrokerResult(broker *Broker) {
	brokerIdStr := strconv.FormatInt(broker.brokerId, 10)
	key := "SmartGoBroker/" + brokerIdStr
	// 由于是目录，因此没有val值，因此val无效，直接用空
	val := ""
	// 超过TTL 10秒钟没有心跳则目录自动被清除
	setOptions := &client.SetOptions{TTL: 10 * time.Duration(time.Second), Dir: true}
	result, err := self.KApi.Set(context.Background(), key, val, setOptions)
	if err != nil {
		log.Printf("[Etcd Kapi err]: %s\n", err)
	}
	log.Printf("[result]: %s\n", result)
	return
}

func (self *EtcdApi) AllBroker() (brokerIdList []int64) {
	key := "SmartGoBroker/"
	getOptions := &client.GetOptions{Recursive: true}
	Response, err := self.KApi.Get(context.Background(), key, getOptions)
	if err != nil {
		log.Printf("[Etcd Kapi err]: %s\n", err)
	}
	// 由于获取的是目录，因此没有val值
	for _, childNode := range Response.Node.Nodes {
		brokeIdStr := childNode.Key[strings.LastIndex(childNode.Key, "/")+1:]
		// string到int64
		brokeId, err := strconv.ParseInt(brokeIdStr, 10, 64)
		if err != nil {
			log.Printf("[Etcd Kapi err]: %s\n", err)
		}
		brokerIdList = append(brokerIdList, brokeId)
	}
	return brokerIdList
}
