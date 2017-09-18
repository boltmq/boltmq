#smartgo
<img src="https://git.oschina.net/uploads/images/2017/0915/133220_def37898_859722.png" width="" height="" alt="SmartGo" align="center" />


### smartgo整体架构图
![](https://git.oschina.net/uploads/images/2017/0915/124147_d695d6e8_859722.png)


### smartgo是什么？
SmartGo是什么是参考AMQP、JMS、Mqtt协议、Kafka，RocketMQ，RabbitMq，Nsq多款中间件，
以及aws，ali、Microsoft等多家云平台开发的智能中间件，
使用golang语言全新开发的一款分布式、队列模型的智能中间件，具有以下特点：

* 支持point-point、pub-sub、request-reply等多种模式
* 支持严格的消息顺序
* 支持数百万长连接
* 亿级消息堆积能力
* 比较友好的分布式特性

当前最新版本功能支持：     
1. 将整个项目命名为smartgo-1.0.0    
2. 将项目中所有子工程命名为stg-*    


----------

### 如何开始？
* [下载最新版代码](https://git.oschina.net/cloudzone/smartgo)
* [`使用指南及相关文档`](https://git.oschina.net/cloudzone/smartgo)都已经归档到当前项目docs/目录下


----------


### 开发规范`必读`
* 源文件使用Unix换行、UTF-8文件编码，遵照golang内置格式化代码规范
* 请在git clone命令之前执行`git config --global core.autocrlf false`，确保本地代码使用Unix换行格式
* 请在非主干分支上开发，禁止提交本地未测试运行通过代码到线上分支
* 每次提交及之前(正常来说需要先pull --rebase,解决冲突)，对代码进行修改必须有相对应的解释说明
* 正常组内开发人员提交代码，需要经过经过审核后方可提交（且需要有统一格式注释，参照注释类型3）
  
### 包管理`必读`
* 包管理工具使用[govendor](https://github.com/kardianos/govendor)

```bash
# 下载并安装包管理工具
go get -u github.com/kardianos/govendor   # 下载govendor源码
go install github.com/kardianos/govendor  # 安装govendor依赖工具

# 同步包
govendor sync  # 基于vendor.json文件下载依赖包

# 更改包依赖
govendor update # 从 $GOPATH 更新包依赖到vendor目录

# 重新做包依赖
govendor init   # 初始化vendor目录
govendor add +external # 添加所有外部包到vendor目录
```



### 注释规范
* 对于注释，请遵照以下规范：
* 注释类型1（适用于结构体或者包名注释）、

```
// 方法对象名 xxx
// Author: xxx, <xxx@gmail.com>
// Since: 2017/3/20 or v1.0.0 
```

* 注释类型2（适用于功能确定的单行注释）、

```
// 由于是顺序消息，因此只能选择一个queue生产和消费消息
```

* 注释类型3（适用于修改它人代码注释）、

```
// xxx  Modify: xxx, <xxx@gmail.com> Since: 2017/3/20 or v1.0.0
// xxx  Add: xxx, <xxx@gmail.com> Since: 2017/3/20 or v1.0.0
```
  
* 关于TODO、FIXME、XXX注释规范（后续再加上）、

```
// TODO: + 说明：xxx Author: xxx, <xxx@gmail.com>  Since: 2017/3/20 or v1.0.0
```
如果代码中有TODO该标识，说明在标识处有功能代码待编写，待实现的功能在说明中会简略说明。

```
// FIXME: + 说明：xxx Author: xxx, <xxx@gmail.com>  Since: 2017/3/20 or v1.0.0
```
如果代码中有FIXME该标识，说明标识处代码需要修正，甚至代码是错误的，不能工作，需要修复，如何修正会在说明中简略说明。

```
// XXX: + 说明：xxx Author: xxx, <xxx@gmail.com>  Since: 2017/3/20 or v1.0.0
```
如果代码中有XXX该标识，说明标识处代码虽然实现了功能，但是实现的方法有待商榷，希望将来能改进，要改进的地方会在说明中简略说明。



### 开发IDE
* 开发工具不做统一规定（Idea、Goland都可以），建议使用Goland
* 建议使用最新版格式Idea，附下载地址：http://pan.baidu.com/s/1slMkXY1
* 附Idea属性格式注释文件下载地址：http://pan.baidu.com/s/1hrU3IgW（其中go版本Idea使用gg或者ggg命令来生成注释，golang使用gg或者ggg）


----------

### 联系我们
 :fa-comments-o: smartgo开发组 https://git.oschina.net/cloudzone/smartgo

----------

