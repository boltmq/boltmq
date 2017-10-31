package static

const (
	REGISTRY_IP               = "0.0.0.0"       // 不能设置为127.0.0.1,否则别的集群无法访问当前机器的registry服务
	REGISTRY_PORT             = 9876            // registry服务端口
	BROKER_IP                 = "0.0.0.0"       // 不能设置为127.0.0.1,否则别的集群无法访问当前机器的broker服务
	BROKER_PORT               = 10911           // broker服务端口
	BROKER_CONFIG_NAME        = "broker-a.toml" // broker启动配置文件
	BROKER_DATA_ROOT_DIR      = "store"         // broker数据根目录
	STORE_COMMIT_LOG_ROOT_DIR = "commitlog"  // store存储数据的commitlog目录
)
