## net

net是BoltMQ中对网络层通讯的封装，包括协议封装、解包，对网络连接的管理和优化，提供高性能的网络基础组件。

### 特性
* 连接管理
* 高连接数
* 高并发
* 长连接
* 粘包
* 协议封装、解包
* 事件通知

### 设计
* 每个连接有单独的goroutine处理接收的报文。
* 每个连接有单独的缓存队列和粘包处理，保证高可用的同时减少资源竞争。
* 粘包后的数据采用多Goroutine处理，提高并发能力。
* 使用对象池降低GC压力。

![Alt text](https://static.oschina.net/uploads/space/2017/1109/170753_f0T7_3724856.jpg "报文处理流程")


### 参考
* io模型(epoll)
* go net
* more

### 待优化
1. 粘包算法，减少粘包开销。
2. 提高消息的缓冲处理能力。
3. 优化协议封装、解包。

Read the [docs](https://boltmq.gitbooks.io/boltmq/content/zh/net)
