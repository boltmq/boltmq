package other

import (
	"context"
	"github.com/coreos/etcd/client"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestCreate(t *testing.T) {
	kApi := getKApi()
	// 超过TTL 10秒钟没有心跳则目录自动被清除
	// 此处如果Dir==true，则对应node.value没有值
	setOptions := &client.SetOptions{TTL: 10 * time.Duration(time.Second), Dir: true}
	clusterId := 1
	idStr := strconv.Itoa(clusterId)
	key := "/mycluster/" + idStr
	val := idStr
	kApi.Set(context.Background(), key, val, setOptions)

	clusterId = 5
	idStr = strconv.Itoa(clusterId)
	key = "/mycluster/" + idStr
	val = idStr
	kApi.Set(context.Background(), key, val, setOptions)

	clusterId = 8
	idStr = strconv.Itoa(clusterId)
	key = "/mycluster/" + idStr
	val = idStr
	result, err := kApi.Set(context.Background(), key, val, setOptions)
	if err != nil {
		log.Printf("[Etcd Kapi err]: %s\n", err)
	}
	if result != nil {
		log.Printf("[result-create]: %s\n", result.Node)
	}
	TestGet(&testing.T{})
}

func TestGet(t *testing.T) {
	kApi := getKApi()
	getOptions := &client.GetOptions{Recursive: true}
	// 获取目录
	key := "/mycluster/"
	result, err := kApi.Get(context.Background(), key, getOptions)
	if err != nil {
		log.Printf("[Etcd Kapi err]: %s\n", err)
	}
	if result != nil {
		log.Printf("[result-get]: %s\n", result.Node.Nodes)
	}
}

// TestWatch watch监听回调
// Author: tantexian, <tantexian@qq.com>
// Since: 2017/7/30
func TestWatch(t *testing.T) {
	TestCreate(&testing.T{})
	kApi := getKApi()
	watcher := &client.WatcherOptions{Recursive: true}
	// 监听目录
	key := "/mycluster/"
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		log.Printf("[Watch]: %s\n", "listing...")
		w := kApi.Watcher(key, watcher)
		TestGet(&testing.T{})
		// next函数会一直阻塞直到监听目录发送变化
		w.Next(context.Background())
		log.Printf("[Watch]: %s\n", " 3------ operation that occurred!!!")
		TestGet(&testing.T{})
		wg.Done()
	}()
	go func() {
		// 休眠三秒钟，在对应目录增加数据子目录/mycluster/188，则会触发w.Next()函数
		time.Sleep(3 * time.Second)
		kApi := getKApi()
		// 超过TTL 10秒钟没有心跳则目录自动被清除
		// 此处如果Dir==true，则对应node.value没有值
		setOptions := &client.SetOptions{TTL: 10 * time.Duration(time.Second), Dir: true}
		clusterId := 188
		idStr := strconv.Itoa(clusterId)
		key := "/mycluster/" + idStr
		val := idStr
		log.Printf("[Watch]: %s\n", " 1------ operation that occurre start!!!")
		kApi.Set(context.Background(), key, val, setOptions)
		log.Printf("[Watch]: %s\n", " 2------ operation that occurre finished!!!")
		wg.Done()
	}()
	wg.Wait()
}

func getKApi() client.KeysAPI {
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
	KApi := client.NewKeysAPI(c)
	return KApi
}
