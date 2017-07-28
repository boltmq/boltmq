#!/bin/sh

# Execute Only Once

#当前程序运行环境用户名
USERNAME=root
#需要修改IO模式磁盘盘符
DISKNAME=sda

echo 'vm.overcommit_memory=1' >> /etc/sysctl.conf
echo 'vm.min_free_kbytes=5000000' >> /etc/sysctl.conf
echo 'vm.drop_caches=1' >> /etc/sysctl.conf
echo 'vm.zone_reclaim_mode=0' >> /etc/sysctl.conf
echo 'vm.max_map_count=655360' >> /etc/sysctl.conf
echo 'vm.dirty_background_ratio=50' >> /etc/sysctl.conf
echo 'vm.dirty_ratio=50' >> /etc/sysctl.conf
echo 'vm.page-cluster=3' >> /etc/sysctl.conf
echo 'vm.dirty_writeback_centisecs=360000' >> /etc/sysctl.conf
echo 'vm.swappiness=10' >> /etc/sysctl.conf
cat /etc/sysctl.conf
sysctl -p

echo 'ulimit -n 655350' >> /etc/profile
echo "$USERNAME hard nofile 655350" >> /etc/security/limits.conf

#DISK=`df -k | sort -n -r -k 2 | awk -F/ 'NR==1 {gsub(/[0-9].*/,"",$3); print $3}'`
#[ "$DISK" = 'cciss' ] && DISK='cciss!c0d0'
echo 'deadline' > /sys/block/$DISKNAME/queue/scheduler


echo "---------------------------------------------------------------"
sysctl vm.overcommit_memory
sysctl vm.min_free_kbytes
sysctl vm.drop_caches
sysctl vm.zone_reclaim_mode
sysctl vm.max_map_count
sysctl vm.dirty_background_ratio
sysctl vm.dirty_ratio
sysctl vm.page-cluster
sysctl vm.dirty_writeback_centisecs
sysctl vm.swappiness

su - $USERNAME -c 'ulimit -n'
cat /sys/block/$DISKNAME/queue/scheduler
