package stgcommon

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
)
// mix_all: 大杂烩
// Author: yintongqiang
// Since:  2017/8/10

const (
	CLOUDMQ_HOME_ENV = "CLOUDMQ_HOME"
	CLOUDMQ_HOME_PROPERTY = "cloudmq.home.dir"
	NAMESRV_ADDR_ENV = "NAMESRV_ADDR"
	NAMESRV_ADDR_PROPERTY = "cloudmq.namesrv.addr"
	MESSAGE_COMPRESS_LEVEL = "cloudmq.message.compressLevel"
	WS_DOMAIN_NAME = "jmenv.tbsite.net"
	WS_DOMAIN_SUBGROUP = "nsaddr"
	// http://jmenv.tbsite.net:8080/rocketmq/nsaddr
	WS_ADDR = "http://" + WS_DOMAIN_NAME + ":8080/cloudmq/" + WS_DOMAIN_SUBGROUP
	DEFAULT_TOPIC = "MY_DEFAULT_TOPIC"
	BENCHMARK_TOPIC = "BenchmarkTest"
	DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER"
	DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER"
	TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER"
	FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER"
	MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER"
	CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER"
	SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP"
	SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP"
	SELF_TEST_TOPIC = "SELF_TEST_TOPIC"
	OFFSET_MOVED_EVENT = "OFFSET_MOVED_EVENT"
	DEFAULT_CHARSET = "UTF-8"
	MASTER_ID = 0
	// 为每个Consumer Group建立一个默认的Topic，前缀 + GroupName，用来保存处理失败需要重试的消息
	RETRY_GROUP_TOPIC_PREFIX = "%RETRY%"
	// 为每个Consumer Group建立一个默认的Topic，前缀 + GroupName，用来保存重试多次都失败，接下来不再重试的消息
	DLQ_GROUP_TOPIC_PREFIX = "%DLQ%"
)
// 压缩
func Compress(src[]byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	defer w.Close()
	w.Write(src)
	w.Flush()
	return b.Bytes()
}
// 解压
func UnCompress(src[]byte) []byte {
	r, _ := gzip.NewReader(bytes.NewBuffer(src))
	defer r.Close()
	data, _ := ioutil.ReadAll(r)
	return data
}
