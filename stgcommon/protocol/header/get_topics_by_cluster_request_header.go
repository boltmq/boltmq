package header

type GetTopicsByClusterRequestHeader struct {
	Cluster string // 集群名称
}

func (header *GetTopicsByClusterRequestHeader) CheckFields() error {
	return nil
}
