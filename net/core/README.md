### 优化策略

#### 程序最大线程数
程序最大线程数的设置，GO进程的默认最大线程数是10000，每个长连接都使用了不同的线程接收数据（之后可能优化）。如果你的业务有成千上万个连接，请main中使用`debug.SetMaxThreads(100000)`配置最大线程数

#### 系统最大连接数
配置服务宿主机系统的最大连接数，以centos 7为例。
临时设置，使用命令
```bash
ulimit -n 655350
```

永久生效，编辑`vi /etc/security/limits.conf`，文件末尾加入：

```bash
* soft nofile 655350
* hard nofile 655350
```
* 表示属于用户，可以指定用户。


#### 系统TCP参数优化（参考，以机器配置和测试结果为准）
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
