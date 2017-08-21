# 日志

### 级别
* Trace(args ...interface{}) 打印trace级别日志
* Debug(args ...interface{}) Debug 打印debug级别日志
* Info(args ...interface{}) 打印info级别日志
* Warn(args ...interface{}) 打印warn级别日志
* Error(args ...interface{}) 打印error级别日志
* Fatal(args ...interface{}) 打印critial级别日志

* Tracef(format string, args ...interface{}) 打印trace级别日志
* Debugf(format string, args ...interface{}) Debug 打印debug级别日志
* Infof(format string, args ...interface{}) 打印info级别日志
* Warnf(format string, args ...interface{}) 打印warn级别日志
* Errorf(format string, args ...interface{}) 打印error级别日志
* Fatalf(format string, args ...interface{}) 打印critial级别日志



### 配置

* 日志配置方法 
```go
func ConfigAsFile(filename string) error
```
注意： **不调用配置方法，日志默认打印到终端。**

* 配置日志打印到文件
```go
<seelog>
    <outputs>
        <file path="test.log"/>
    </outputs>
</seelog>
```

### 代码示例
```go
package main

import "git.oschina.net/cloudzone/smartgo/stgcommon/logger"

func main() {
	console()
}

func console() {
	logger.Trace("this is a test")
	logger.Debug("this is a test")
	logger.Info("this is a test")
	logger.Warn("this is a test")
	logger.Error("this is a test")
	logger.Fatal("this is a test")

	logger.Tracef("this is a %s", "test")
	logger.Debugf("this is a %s", "test")
	logger.Infof("this is a %s", "test")
	logger.Warnf("this is a %s", "test")
	logger.Errorf("this is a %s", "test")
	logger.Fatalf("this is a %s", "test")
}
```

```go
package main

import "git.oschina.net/cloudzone/smartgo/stgcommon/logger"

func main() {
	file()
}

func file() {
	logger.ConfigAsFile("seelog.xml")

	logger.Trace("this is a test")
	logger.Debug("this is a test")
	logger.Info("this is a test")
	logger.Warn("this is a test")
	logger.Error("this is a test")
	logger.Fatal("this is a test")

	logger.Tracef("this is a %s", "test")
	logger.Debugf("this is a %s", "test")
	logger.Infof("this is a %s", "test")
	logger.Warnf("this is a %s", "test")
	logger.Errorf("this is a %s", "test")
	logger.Fatalf("this is a %s", "test")
}
```
