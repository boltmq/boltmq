package namesrv

// GetKVConfigRequestHeader: 创建头请求信息
// Author: yintongqiang
// Since:  2017/8/23
type GetKVConfigRequestHeader struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

func (header *GetKVConfigRequestHeader) CheckFields() error {
	return nil
}
