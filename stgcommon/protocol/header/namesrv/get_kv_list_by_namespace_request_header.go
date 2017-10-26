package namesrv

import (
	"strings"
	"fmt"
)

type GetKVListByNamespaceRequestHeader struct {
	Namespace string
}

func (header *GetKVListByNamespaceRequestHeader) CheckFields() error {
	if strings.TrimSpace(header.Namespace) == "" {
		return fmt.Errorf("GetKVListByNamespaceRequestHeader.Namespace is empty")
	}
	return nil
}

