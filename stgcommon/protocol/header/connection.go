package header

// Connection 连接信息
// Author rongzhihong
// Since 2017/9/19
type Connection struct {
	ClientId   string `json:"clientId"`   // 客户端实例
	ClientAddr string `json:"clientAddr"` // 客户端地址
	Language   string `json:"language"`   // 开发语言
	Version    int32  `json:"version"`    // mq发行版本号
}
