package header

type GetTopicsByClusterRequestHeader struct {
	Cluster string `json:"cluster"` // 集群名称
	Extra   bool   `json:"extra"`   // 是否额外查询topic、cluster对应关系
}

func (header *GetTopicsByClusterRequestHeader) CheckFields() error {
	return nil
}
