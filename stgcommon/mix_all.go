package stgcommon

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/fileutil"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// mix_all: 大杂烩
// Author: yintongqiang
// Since:  2017/8/10
const (
	SMARTGO_HOME_ENV                = "SMARTGO_HOME"            // smartgo home目录
	CLOUDMQ_HOME_PROPERTY           = "smartgo.home.dir"        // 默认smartgo home地址
	NAMESRV_ADDR_ENV                = "NAMESRV_ADDR"            // namesrv地址环境变量
	NAMESRV_PORT_ENV                = "NAMESRV_PORT"            // namesrv端口环境变量
	NAMESRV_ADDR_PROPERTY           = "cloudmq.namesrv.addr"    // 默认namesrv_addr地址
	SMARTGO_DATA_PATH_ENV           = "SMARTGO_DATA_PATH"       // broker、store等模块，存取数据的目录
	SMARTGO_REGISTRY_CONFIG_ENV     = "SMARTGO_REGISTRY_CONFIG" // registry模块的日志配置文件路径
	BLOTMQ_WEB_CONFIG_ENV           = "BLOTMQ_WEB_CONFIG"       // console控制台web界面的配置文件
	MESSAGE_COMPRESS_LEVEL          = "cloudmq.message.compressLevel"
	WS_DOMAIN_NAME                  = "jmenv.tbsite.net"
	WS_DOMAIN_SUBGROUP              = "nsaddr"
	WS_ADDR                         = "http://" + WS_DOMAIN_NAME + ":8080/smartgo/" + WS_DOMAIN_SUBGROUP // http://jmenv.tbsite.net:8080/smartgo/nsaddr
	DEFAULT_TOPIC                   = "MY_DEFAULT_TOPIC"
	BENCHMARK_TOPIC                 = "BenchmarkTest"
	DEFAULT_PRODUCER_GROUP          = "DEFAULT_PRODUCER"
	DEFAULT_CONSUMER_GROUP          = "DEFAULT_CONSUMER"
	TOOLS_CONSUMER_GROUP            = "TOOLS_CONSUMER"
	FILTERSRV_CONSUMER_GROUP        = "FILTERSRV_CONSUMER"
	MONITOR_CONSUMER_GROUP          = "__MONITOR_CONSUMER"
	CLIENT_INNER_PRODUCER_GROUP     = "CLIENT_INNER_PRODUCER"
	SELF_TEST_PRODUCER_GROUP        = "SELF_TEST_P_GROUP"
	SELF_TEST_CONSUMER_GROUP        = "SELF_TEST_C_GROUP"
	SELF_TEST_TOPIC                 = "SELF_TEST_TOPIC"
	OFFSET_MOVED_EVENT              = "OFFSET_MOVED_EVENT"
	DEFAULT_CHARSET                 = "UTF-8"
	MASTER_ID                       = 0
	RETRY_GROUP_TOPIC_PREFIX        = "%RETRY%" // 为每个ConsumerGroup建立一个默认的Topic，前缀+GroupName，用来保存处理失败需要重试的消息
	DLQ_GROUP_TOPIC_PREFIX          = "%DLQ%"   // 为每个ConsumerGroup建立一个默认的Topic，前缀+GroupName，用来保存重试多次都失败，接下来不再重试的消息
	BROKER_REBLANCE_LOCKMAXLIVETIME = "smartgo.broker.rebalance.lockMaxLiveTime"
	SMARTGO_CONF_DIR                = "/git.oschina.net/cloudzone/smartgo/conf/"
	MSG_BODY_DIR                    = "/tmp/blotmq/msgbodys/" // 消息body内容存储在stgweb站点所在服务器路径
)

func GetRetryTopic(consumerGroup string) string {
	return RETRY_GROUP_TOPIC_PREFIX + consumerGroup
}

// 压缩
func Compress(src []byte) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	defer w.Close()
	w.Write(src)
	w.Flush()
	return b.Bytes()
}

// 解压
func UnCompress(src []byte) []byte {
	r, _ := gzip.NewReader(bytes.NewBuffer(src))
	defer r.Close()
	data, _ := ioutil.ReadAll(r)
	return data
}

func CompareAndIncreaseOnly(target *int64, value int64) bool {
	if value > *target {
		updated := atomic.CompareAndSwapInt64(target, *target, value)
		if updated {
			return true
		}
	}
	return false
}

func GetDLQTopic(consumerGroup string) string {
	return DLQ_GROUP_TOPIC_PREFIX + consumerGroup
}

func HashCode(s string) int64 {
	var h int64
	for i := 0; i < len(s); i++ {
		h = 31*h + int64(s[i])
	}
	return h
}

// UnixNano return current time unix
func UnixNano() int64 {
	return time.Now().UnixNano()
}

// GetCurrentTimeMillis 得到当前时间的毫秒数
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func GetCurrentTimeMillis() (currentTimeMillis int64) {
	currentTimeMillis = time.Now().UnixNano() / int64(time.Millisecond)
	return currentTimeMillis
}

// FormatTimestamp 转化时间戳为字符串(自动适配毫秒数)
//
// 使用示例
// (1)FormatTimestamp(1505716870) 		==> 2017/9/18 14:41:10
//
// (2)FormatTimestamp(1505716870921) 	==> 2017/9/18 14:41:10.921
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/18
func FormatTimestamp(stamp int64) string {
	// 普通日期转为秒级时间戳,长度10位(eg. 2017/9/18 14:41:10 ==> 1505716870)
	// 普通日期转为毫秒时间戳,长度13位(eg. 2017/9/18 14:41:10.921 ==> 1505716870921)
	if tmp := strconv.FormatInt(stamp, 10); len(tmp) == 10 {
		stamp = stamp * 1000 // 如果当前时间戳是秒，那就转化为毫秒级的时间戳
	}

	sec := stamp / int64(time.Microsecond)
	nsec := stamp % int64(time.Microsecond)

	t := time.Unix(sec, nsec*int64(time.Millisecond))
	if t.Year() == 1 {
		return ""
	}

	timeLayout := "2006-01-02 15:04:05"
	if nsec > 0 {
		timeLayout = "2006-01-02 15:04:05.000"
	}
	return t.Format(timeLayout)
}

// 写文件 2017/8/28 Add by yintongjiang,windows"\\"需改成"/"
func String2File(data []byte, fileName string) {
	tmpFile := fileName + ".tmp"
	createFile(data, tmpFile)
	bakFile := fileName + ".bak"
	oldData, err := ioutil.ReadFile(fileName)
	if err == nil {
		createFile(oldData, bakFile)
	}
	// 删除原文件
	os.Remove(fileName)
	// 重命临时文件
	os.Rename(tmpFile, fileName)
}

func createFile(data []byte, fileName string) {
	err := fileutil.EnsureDir(fileName)
	if err != nil {
		logger.Errorf("EnsureDir error=%v ", err.Error())
	}
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("create file error ", err.Error())
	}
	file.Write(data)
	defer file.Close()
}

// File2String 读取文件内容
func File2String(filePath string) (data string, err error) {
	bf, err := file2String(filePath)
	if err != nil {
		return "", err
	}

	return string(bf), nil
}

func file2String(filePath string) (bf []byte, err error) {
	bf, err = ioutil.ReadFile(filePath)
	if err != nil {
		return []byte{}, err
	}

	return bf, nil
}

// CreateDir 创建文件夹
func CreateDir(dir string) (bool, error) {
	if err := os.MkdirAll(dir, os.FileMode(os.O_CREATE)); err != nil {
		return false, err
	}

	return true, nil
}

// CreateFile 创建文件
func CreateFile(fileFullName string) (bool, error) {
	parentDir := filepath.Dir(fileFullName)
	_, err := CreateDir(parentDir)
	if err != nil {
		return false, err
	}

	_, err = os.Create(fileFullName)
	if err != nil {
		return false, err
	}

	return true, nil
}

// ExistsDir 校验文件是否存在
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/15
func ExistsDir(fileFullPath string) (bool, error) {
	fileinfo, err := os.Stat(fileFullPath)
	if err == nil {
		return fileinfo.IsDir(), nil // 文件夹存在
	}
	if os.IsNotExist(err) {
		return false, nil // 使用os.IsNotExist()判断为true,说明文件或文件夹不存在
	}

	return false, err // 不确定是否在存在
}

// ExistsFile 校验文件是否存在
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/15
func ExistsFile(fileFullPath string) (bool, error) {
	fileinfo, err := os.Stat(fileFullPath)
	if err == nil {
		return !fileinfo.IsDir(), nil // 文件是否存在
	}
	if os.IsNotExist(err) {
		return false, nil // 使用os.IsNotExist()判断为true,说明文件或文件夹不存在
	}
	return false, err // 不确定是否在存在
}

// GetGoPath 获取GoPath路径
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func GetGoPath() string {
	return strings.TrimSpace(os.Getenv("GOPATH"))
}

// GetNamesrvAddr 获取环境变量“NAMESRV_ADDR”的值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func GetNamesrvAddr() string {
	return strings.TrimSpace(os.Getenv(NAMESRV_ADDR_ENV))
}

// GetNamesrvPort 获取环境变量“NAMESRV_PORT”的值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func GetNamesrvPort() string {
	return strings.TrimSpace(os.Getenv(NAMESRV_PORT_ENV))
}

// GetSmartGoHome 获取环境变量“SMARTGO_HOME”的值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func GetSmartGoHome() string {
	return strings.TrimSpace(os.Getenv(SMARTGO_HOME_ENV))
}

// GetUserHomeDir 获取当前操作系统登陆用户的Home目录
//
// 注意：
// 	(1)一台服务器，启动多个broker，因此就需要环境变量“SMARTGO_DATA_PATH”来区别每个broker的数据目录
//  (2)如果配置了环境变量“SMARTGO_DATA_PATH”，那么user.Current().HomeDir的路径会被覆盖
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func GetUserHomeDir() string {
	storePathRootDir := strings.TrimSpace(os.Getenv(SMARTGO_DATA_PATH_ENV))
	if storePathRootDir != "" {
		return storePathRootDir
	}

	currentUser, _ := user.Current()
	return currentUser.HomeDir
}

// GetSmartRegistryConfig 获取环境变量“SMARTGO_REGISTRY_CONFIG”的值
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func GetSmartRegistryConfig() string {
	return strings.TrimSpace(os.Getenv(SMARTGO_REGISTRY_CONFIG_ENV))
}

// GetSmartgoConfigDir 为了IDEA开发调试，得到当前项目conf配置项路径,路径末尾带上"/"字符
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func GetSmartgoConfigDir(config ...interface{}) string {
	gopath := GetGoPath()
	src := "/src"

	dirPath := SMARTGO_CONF_DIR
	if config != nil && len(dirPath) > 0 {
		dirPath = "/" + reflect.TypeOf(config[0]).PkgPath() + "/"
	}
	smartgoConfigPath := gopath + src + dirPath
	return smartgoConfigPath
}

// CheckIpAndPort 校验ip:port是否有效
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func CheckIpAndPort(addr string) bool {
	if addr == "" {
		return false
	}
	ipAndPort := strings.Split(addr, ":")
	if ipAndPort == nil || len(ipAndPort) != 2 {
		return false
	}
	if tmpIp := net.ParseIP(ipAndPort[0]); tmpIp == nil {
		return false
	}
	if tmpPort, err := strconv.Atoi(ipAndPort[1]); err != nil || (tmpPort <= 1 || tmpPort >= 100000) {
		return false
	}
	return true
}

// ParseClientAddr 转化客户端地址
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/27
func ParseClientAddr(clientAddr string) (ip string, pid int) {
	if clientAddr == "" {
		return "", 0
	}

	val := strings.Split(clientAddr, ":")
	if val == nil || len(val) != 2 {
		return "", 0
	}

	ip = val[0]
	pid, err := strconv.Atoi(val[1])
	if err != nil {
		return "", 0
	}
	return ip, pid
}
