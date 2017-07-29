#!/usr/bin/env bash

# 以下目前仅支持centos7系统
yum install etcd -y

# 手动修改配置文件
# ETCD_LISTEN_CLIENT_URLS="http://0.0.0.0:2379"
# ETCD_ADVERTISE_CLIENT_URLS="http://192.168.8.8:2379"
# ETCD_INITIAL_ADVERTISE_PEER_URLS="http://192.168.8.8:2380"
# vim /etc/etcd/etcd.conf

# 开机自启动
systemctl enable etcd

# 启动etcd服务
systemctl restart etcd
