package logger

// LogEngine 日志模式配置
type LogEngine struct {
	Adapter string                 `json:"adapter"`
	Config  map[string]interface{} `json:"config"`
}

// Config 日志配置
type Config struct {
	Engine              LogEngine `json:"engine"`
	Path                string    `json:"path"`
	CacheSize           int64     `json:"cache_size"`
	EnableFuncCallDepth bool      `json:"enable_func_call_depth"`
	FuncCallDepth       int       `json:"func_call_depth"`
}

