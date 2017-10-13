## smartgo registry


Read the [docs](http://git.oschina.net/cloudzone/smartgo)

### 环境变量
```bash
export SMARTGO_REGISTRY_CONFIG="/home/smartgo/registry/cfg.json" # registry日志配置文件
```

### logger日志
- 日志组件为`github.com/astaxie/beego/logs`，已修改它的`logger.go`文件，支持第一列时间字段的毫秒显示效果
- `registry`启动后，优先读取`$SMARTGO_REGISTRY_CONFIG`日志配置文件，如果读取异常则尝试读取`cfg.json`默认路径
- 如果`cfg.json`默认文件还是读取异常，则会将日志输出到控制台，日志级别默认为`Info`
```bash
$GOPATH/src/git.oschina.net/cloudzone/smartgo/stgregistry/start/g/cfg.json   # 日志默认路径
```

### 日志文件`cfg.json`示例
```json
{
  "log": {
    "engine": {
      "adapter": "file",
      "config": {
        "filename": "./logs/registry.log",
        "level":6,
        "maxdays":30,
        "enableFuncCallDepth":false,
        "loggerFuncCallDepth":3
      }
    }
  }
}
```

### 日志文件`cfg.json`说明

| 字段	       | 默认值        | 参数说明   | 
|:-----------  |:-------------| :-----|
| adapter      | file  | 日志终端类型， 文件file、控制台console  |
| filename     | `./logs/registry.log` | 日志路径， 如果adapter为控制台，此字段无效|
| level        | 6   | 日志级别 7:debug, 6:info, 4:warn, 3:error    |
| maxdays      | 30 | 每天一个日志文件，最多保留文件个数  |
| enableFuncCallDepth| false  |打印日志的同时，是否打印该日志对应的 源码文件名与行号   |
| loggerFuncCallDepth| 3|  默认打印堆栈异常新的层数 |



### 编译`registry`
```bash
cd $GOPATH/src/git.oschina.net/cloudzone/smartgo/stgregistry/start
go get ./...
go build 
mv start registry
```


### 启动`registry`
```bash
mkdir ./logs
touch logs/registry.log  # 第一次启动，确保./logs/registry.log文件存在
nohup ./registry &
```

### 查看`registry`日志
```bash
tailf nohup.out
tailf registry.log
```

## 信号类型

一个平台的信号定义或许有些不同。下面列出了POSIX中定义的信号。
Linux使用34-64信号用作实时系统中。
命令 man 7 signal 提供了官方的信号介绍。
在POSIX.1-1990标准中定义的信号列表, 更多资料，参考 [信号类型](http://www.cnblogs.com/jkkkk/p/6180016.html)

| 信号	       | 值            | 值   | 说明   |
| :----------- |:-------------| :-----|:-----|
| SIGHUP        | 1          | Term   | 终端控制进程结束(终端连接断开)   |
| SIGINT        | 2          | Term   | 用户发送INTR字符(Ctrl+C)触发  |
| SIGQUIT        | 3          | Core   | 用户发送QUIT字符(Ctrl+/)触发   |
| SIGILL        | 4          | Core   | 非法指令(程序错误、试图执行数据段、栈溢出等)  |
| SIGABRT        | 6          | Core   | 调用abort函数触发   |
| SIGFPE        | 8          | Core   | 算术运行错误(浮点运算错误、除数为零等) |
| SIGKILL        | 9          | Term   | 无条件结束程序(不能被捕获、阻塞或忽略)   |
| SIGSEGV        | 11          | Core   | 无效内存引用(试图访问不属于自己的内存空间、对只读内存空间进行写操作)   |
| SIGPIPE        | 13          | Term   | 消息管道损坏(FIFO/Socket通信时，管道未打开而进行写操作)  |
| SIGALRM        | 14          | Term   | 时钟定时信号  |
| SIGTERM        | 15          | Term   | 结束程序(可以被捕获、阻塞或忽略) |

