package namesrv

// GetKVConfigResponseHeader: 响应头
// Author: yintongqiang
// Since:  2017/8/23
type GetKVConfigResponseHeader struct {
	Value string `json:"value"`
}

func (header *GetKVConfigResponseHeader) CheckFields() error {
	return nil
}
