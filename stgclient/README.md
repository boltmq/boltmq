## stgclient

Read the [docs](http://git.oschina.net/cloudzone/smartgo)

### 创建topic
* 1、```import "git.oschina.net/cloudzone/smartgo/stgclient/process"```
* 2、创建发送实例```process.NewDefaultMQProducer("producerGroupId")```
* 3、设置stgregistry地址```SetNamesrvAddr(namesrvAddr)```
* 4、建立链接调用```Start()```方法
* 5、调用创建topic的方法```CreateTopic(stgcommon.DEFAULT_TOPIC, "topicName", 8)```
    * ```stgcommon.DEFAULT_TOPIC``` 为密钥需先```import "git.oschina.net/cloudzone/smartgo/stgcommon"```
    * ```topicName``` 为创建topic的名称
    * ```8```为读写的队列数
    
### 发送同步消息
* #### 请求
* 1、``` import "git.oschina.net/cloudzone/smartgo/stgclient/process" ```
* 2、创建发送实例```process.NewDefaultMQProducer("producerGroupId")```
* 3、设置stgregistry地址```SetNamesrvAddr(namesrvAddr)```
* 4、建立链接调用```Start()```方法
* 5、调用实例的Send方法```Send(message.NewMessage("topicName", "tagName", []byte("msgbody")))```
    * 先```import "git.oschina.net/cloudzone/smartgo/stgcommon/message"``` 
    * 创建Message的参数topicName 为创建topic的名称,tagName为标签名称,msgBody为消息内容。
    
* #### 响应    
  * Send方法返回值为```(*SendResult, error)```
  * SendResult中SendStatus值说明
     * ```SEND_OK``` 发送成功并同步到SLAVE成功
     * ```FLUSH_DISK_TIMEOUT``` 刷盘超时
     * ```FLUSH_SLAVE_TIMEOUT``` 同步到SLAVE超时 
     * ```SLAVE_NOT_AVAILABLE SLAVE```不可用
     
### 发送异步消息
* #### 请求
* 1、``` import "git.oschina.net/cloudzone/smartgo/stgclient/process" ```
* 2、创建发送实例```process.NewDefaultMQProducer("producerGroupId")```
* 3、设置stgregistry地址```SetNamesrvAddr(namesrvAddr)```
* 4、建立链接调用```Start()```方法
* 5、调用实例的SendCallBack方法```SendCallBack(message.NewMessage("topicName", "tagName", []byte("msgbody")),
    func(sendResult *process.SendResult, err error) {})```                                                                                             		
    * 先```import "git.oschina.net/cloudzone/smartgo/stgcommon/message"``` 
    * 创建Message的参数topicName 为创建topic的名称,tagName为标签名称,msgBody为消息内容。
    
* #### 响应    
  * SendCallBack回调函数```func(sendResult *process.SendResult, err error)```
  * SendResult中SendStatus值说明
     * ```SEND_OK``` 发送成功并同步到SLAVE成功
     * ```FLUSH_DISK_TIMEOUT``` 刷盘超时
     * ```FLUSH_SLAVE_TIMEOUT``` 同步到SLAVE超时 
     * ```SLAVE_NOT_AVAILABLE SLAVE```不可用     
     
     
### 发送OneWay消息
* #### 请求
* 1、``` import "git.oschina.net/cloudzone/smartgo/stgclient/process" ```
* 2、创建发送实例```process.NewDefaultMQProducer("producerGroupId")```
* 3、设置stgregistry地址```SetNamesrvAddr(namesrvAddr)```
* 4、建立链接调用```Start()```方法
* 5、调用实例的SendOneWay方法```SendOneWay(message.NewMessage("topicName", "tagName", []byte("msgbody")))```
    * 先```import "git.oschina.net/cloudzone/smartgo/stgcommon/message"``` 
    * 创建Message的参数topicName 为创建topic的名称,tagName为标签名称,msgBody为消息内容。
    
* #### 响应    
  * SendOneWay有错误返回```error```
  
  
### Push消费

* 1、``` import "git.oschina.net/cloudzone/smartgo/stgclient/process" ```
* 2、创建消费实例```process.NewDefaultMQPushConsumer("consumerGroupId")```
* 3、设置实例消费位置```SetConsumeFromWhere(heartbeat.CONSUME_FROM_LAST_OFFSET)```
     * 先 ```import "git.oschina.net/cloudzone/smartgo/stgcommon/protocol/heartbeat"```
     * ```CONSUME_FROM_LAST_OFFSET```  一个新的订阅组第一次启动从队列的最后位置开始消费,后续再启动接着上次消费的进度开始消费。
     * ```CONSUME_FROM_FIRST_OFFSET``` 一个新的订阅组第一次启动从队列的最前位置开始消费,后续再启动接着上次消费的进度开始消费。
     * ```CONSUME_FROM_TIMESTAMP```  一个新的订阅组第一次启动从指定时间点开始消费,后续再启动接着上次消费的进度开始消费,时间点设置参见```DefaultMQPushConsumer.ConsumeTimestamp```参数。
* 4、设置消费模式```SetMessageModel(heartbeat.CLUSTERING)```
     * ```CLUSTERING``` 集群消费。
     * ```BROADCASTING``` 广播消费。
* 5、设置stgregistry地址```SetNamesrvAddr(namesrvAddr)```
* 6、设置订阅topic和tag```Subscribe("topicName", "tagName")```
* 6、设置监听器```RegisterMessageListener(&MessageListenerImpl{})```
     * 普通消息需```MessageListenerImpl```实现```MessageListenerConcurrently```的接口
     * 顺序消息需```MessageListenerImpl```实现```MessageListenerOrderly```的接口
* 7、建立链接调用```Start()```方法


### Pull消费

* 1、``` import "git.oschina.net/cloudzone/smartgo/stgclient/process" ```
* 2、创建消费实例```process.NewDefaultMQPullConsumer("consumerGroupId")```
* 3、设置stgregistry地址```SetNamesrvAddr(namesrvAddr)```
* 4、建立链接调用```Start()```方法 
* 5、调用实例的```FetchSubscribeMessageQueues("topicName")```方法，拿到topic的所有队列
* 6、调用实例的```Pull(mq, "tagA", 0, 32)```方法
     * ```mq```为队列结构体
     * ```tagA```为tag的标签名称
     * ```0```为该队列offset，需自行维护
     * ```32```为一次拉取数量。