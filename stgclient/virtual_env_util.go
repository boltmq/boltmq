package stgclient

import (
	"fmt"
	"strings"
)

const (
	VIRTUAL_APPGROUP_PREFIX = "%%PROJECT_%s%%"
)

// VirtualEnvUtil: 客户端工具
// Author: yintongqiang
// Since:  2017/8/10

func BuildWithProjectGroup(origin string, projectGroup string) string {
	if !strings.EqualFold(projectGroup, "") {
		prefix := fmt.Sprintf(VIRTUAL_APPGROUP_PREFIX, projectGroup)
		if !strings.HasSuffix(origin, prefix) {
			return strings.Join([]string{origin, prefix}, "")
		} else {
			return origin
		}
	} else {
		return origin
	}
}

func ClearProjectGroup(origin string, projectGroup string) string {
	prefix := fmt.Sprintf(VIRTUAL_APPGROUP_PREFIX, projectGroup)
	if !strings.EqualFold(projectGroup, "") && strings.HasSuffix(origin, prefix) {
		return origin[0:strings.Index(origin, prefix)]
	}
	return origin
}
