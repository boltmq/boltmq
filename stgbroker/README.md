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
cp $GOPATH/src/git.oschina.net/cloudzone/smartgo/conf/smartgoBroker.toml $SMARTGO_HOME/conf/
mkdir ./logs
touch ./logs/broker.log  # 第一次启动确保./logs/registry.log文件存在
nohup ./broker &
```

### 多实例启动`broker`
```bash
# 编辑smartgoBroker.toml, 确保每个brokerRole、brokerId不相同
# (1)如果在一台服务器启动多个broker实例，请在smartgoBroker.toml文件的brokerPort字段更新监听端口
# (2)如果在一台服务器启动多个broker实例，请在smartgoBroker.toml文件的smartgoDataPath字段设置不同实例的数据目录
vim $SMARTGO_HOME/conf/smartgoBroker.toml

cd $SMARTGO_HOME
mkdir -p $SMARTGO_HOME/conf
cp $GOPATH/src/git.oschina.net/cloudzone/smartgo/conf/smartgoBroker.toml $SMARTGO_HOME/conf/
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