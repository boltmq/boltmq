package filtersrv

// RegisterFilterServerRequestHeader 注册过滤器的请求头
// Author rongzhihong
// Since 2017/9/19
type RegisterFilterServerRequestHeader struct {
	FilterServerAddr string `json:"filterServerAddr"`
}

func (header *RegisterFilterServerRequestHeader) CheckFields() error {
	return nil
}
