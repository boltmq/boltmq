package stgclient

import (
	"strings"
	"fmt"
)
// VirtualEnvUtil: 客户端工具
// Author: yintongqiang
// Since:  2017/8/10
const (
	VIRTUAL_APPGROUP_PREFIX = "%%PROJECT_%s%%";
)

func BuildWithProjectGroup(origin string, projectGroup string) string {
	if !strings.EqualFold(projectGroup, "") {
		prefix:= fmt.Sprintf(VIRTUAL_APPGROUP_PREFIX, projectGroup)
		if !strings.HasSuffix(origin, prefix) {
			return strings.Join([]string{origin, prefix},"")
		} else {
			return origin
		}
	} else {
		return origin
	}
}