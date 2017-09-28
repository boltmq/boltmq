package header

// Connection 连接信息
// Author rongzhihong
// Since 2017/9/19
type Connection struct {
	ClientId   string `json:"clientId"`
	ClientAddr string `json:"clientAddr"`
	Language   string `json:"language"`
	Version    int32  `json:"version"`
}
