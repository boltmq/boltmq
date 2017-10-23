## smartgo broker


Read the [docs](http://git.oschina.net/cloudzone/smartgo)

### 环境变量
```bash
export SMARTGO_HOME="/home/smartgo/" # registry日志配置文件
```

### 编译`broker`
```bash
cd $GOPATH/src/git.oschina.net/cloudzone/smartgo/stgbroker/start
go get ./...
go build 
mv start broker
```

### 单实例默认启动`broker`
```bash
cd $SMARTGO_HOME
mkdir -p $SMARTGO_HOME/conf
cp $GOPATH/src/git.oschina.net/cloudzone/smartgo/conf/broker-a.toml $SMARTGO_HOME/conf/
mkdir ./logs
touch ./logs/broker.log  # 第一次启动确保./logs/registry.log文件存在
nohup ./broker &
```

### 多实例启动`broker`
```bash
# 编辑broker-a.toml, 确保每个brokerRole、brokerId不相同
# (1)如果在一台服务器启动多个broker实例，请在broker-a.toml文件的brokerPort字段更新监听端口
# (2)如果在一台服务器启动多个broker实例，请在broker-a.toml文件的smartgoDataPath字段设置不同实例的数据目录
vim $SMARTGO_HOME/conf/smartgoBroker.toml

cd $SMARTGO_HOME
mkdir -p $SMARTGO_HOME/conf
cp $GOPATH/src/git.oschina.net/cloudzone/smartgo/conf/broker-a.toml $SMARTGO_HOME/conf/
mkdir ./logs
touch ./logs/broker.log  # 第一次启动确保./logs/broker.log文件存在

# 启动brokerA实例
nohup ./brokerA -c $SMARTGO_HOME/conf/smartgoBrokerA.toml &

# 启动brokerB实例
nohup ./brokerB -c $SMARTGO_HOME/conf/smartgoBrokerB.toml &
```


### 查看`broker`日志
```bash
tailf nohup.out
tailf ./logs/broker.log
```


### `smartgo`部署包结构
```
smartgo/
├── stgbroker
│   ├── broker
│   ├── conf
│   │   └── broker-a.toml
│   ├── logs
│   │   └── broker.log
│   └── store
│       ├── abort
│       ├── checkpoint
│       ├── commitlog
│       ├── config
│       │   ├── consumerOffset.json
│       │   ├── consumerOffset.json.bak
│       │   ├── subscriptionGroup.json
│       │   ├── topics.json
│       │   └── topics.json.bak
│       └── consumequeue
├── stgclient
│   ├── consumer
│   │   └── push
│   ├── producer
│   │   └── producer
│   ├── topic
│   │   └── topic
│   └── tps
│       └── tps
└── stgregistry
    ├── cfg.json
    ├── logs
    │   └── registry.log
    ├── nohup.out
    └── register
    
```

### 环境变量`SMARTGO_HOME`与`-c`指令说明问题
* 1.`-c`指令优先级高于`SMARTGO_HOME`环境变量
* 2.启动命令如果是`./broker -c xxx.toml`，则优先使用`-c`指令对应的toml文件
* 3.如果`-c`之类对应的toml找不到，并且已配置`SMARTGO_HOME`环境变量，则尝试读取`$SMARTGO_HOME/conf/broker-a.toml`
* 4.如果`$SMARTGO_HOME/conf/broker-a.toml`读取失败，则尝试读取`./conf/broker-a.toml`
* 5.如果`./conf/broker-a.toml`读取失败，则读取`$GOPATH/src/git.oschina.net/cloudzone/smartgo/conf/broker-a.toml`
* 6.如果以上步骤都无法读取toml文件，则启动broker报错

### 关于IDEA编辑器特殊启动问题
* IDEA的 golang-sdk，在windows系统执行目录是`C:\\Users\\xxxxx\\AppData\\Local\\Temp`
* 配置环境变量`SMARTGO_HOME="$GOPATH/src/git.oschina.net/cloudzone/smartgo/"`
* 通过IDEA编辑器启动broker后自动寻找`$SMARTGO_HOME/conf/broker-a.toml`配置文件

### broker数据目录、监听端口
在测试服务器不够的情况下，需要在1台服务器启动多个broker，因此需要配置多个broker的数据目录、以及各自broker的监听端口
* broker监听端口，支持在toml文件```brokerPort```字段配置。如果不配置，则默认监听```10911```端口
* broker数据目录，支持在toml文件```storePathRootDir```字段配置。如果不配置，则默认目录是```$HOME/store```



 