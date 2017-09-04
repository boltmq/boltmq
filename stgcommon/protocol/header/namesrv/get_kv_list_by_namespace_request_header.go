package namesrv

type GetKVListByNamespaceRequestHeader struct {
	Namespace string
}

func (header *GetKVListByNamespaceRequestHeader) CheckFields() error {
	return nil
}

