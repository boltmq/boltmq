## smart net

stgnet是smartgo中对网络层通讯的封装，包括协议封装、解包，对网络连接的管理和优化，提供高性能的网络基础组件。

### 特性
* 连接管理
* 高连接数
* 高并发
* 长连接
* 粘包
* 协议封装、解包
* 事件通知

### 设计
![Alt text](https://static.oschina.net/uploads/space/2017/1109/170119_u8FX_3724856.jpg)
<img src="https://static.oschina.net/uploads/space/2017/1109/170119_u8FX_3724856.jpg" width="" height="" alt="SmartGo" align="center" />


* 每个连接有单独的goroutine处理接收的报文。
* 每个连接有单独的缓存队列和粘包处理，保证高可用的同时减少资源竞争。
* 粘包后的数据采用多Goroutine处理，提高并发能力。
* 使用对象池降低GC压力。

### 优化

#### linux系统

**系统最大连接数**
```bash
ulimit -n 655350
```
或者

永久生效,`vi /etc/security/limits.conf`，文件末尾加入：
```bash
* soft nofile 655350
* hard nofile 655350
```

**TCP参数优化**
编辑`/etc/sysctl.conf`

```bash
net.ipv4.ip_local_port_range = 1024 65536
net.core.rmem_max=16777216
net.core.wmem_max=16777216
net.ipv4.tcp_rmem=4096 87380 16777216
net.ipv4.tcp_wmem=4096 65536 16777216
net.ipv4.tcp_fin_timeout = 10
net.ipv4.tcp_tw_recycle = 1
net.ipv4.tcp_timestamps = 0
net.ipv4.tcp_window_scaling = 0
net.ipv4.tcp_sack = 0
net.core.netdev_max_backlog = 30000
net.ipv4.tcp_no_metrics_save=1
net.core.somaxconn = 262144
net.ipv4.tcp_syncookies = 0
net.ipv4.tcp_max_orphans = 262144
net.ipv4.tcp_max_syn_backlog = 262144
net.ipv4.tcp_synack_retries = 2
net.ipv4.tcp_syn_retries = 2
```

生效命令:
```bash
sysctl -p /etc/sysctl.conf
sysctl -w net.ipv4.route.flush=1
```

### 待优化
1. 粘包算法，减少粘包开销。
2. 提高消息的缓冲处理能力。
3. 优化协议封装、解包。
4. 短连接支持。

Read the [docs](http://git.oschina.net/cloudzone/smartgo)
