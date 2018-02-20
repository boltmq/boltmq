// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/boltmq/common/basis"
	"github.com/boltmq/common/net/core"
)

// min int64 的最小值
// Author rongzhihong
// Since 2017/9/18
func min(a, b int64) int64 {
	if a >= b {
		return b
	}
	return a
}

// callShell 执行命令
// Author rongzhihong
// Since 2017/9/8
func CallShell(shellString string) error {
	process := exec.Command(shellString)
	err := process.Start()
	if err != nil {
		return err
	}

	err = process.Wait()
	if err != nil {
		return err
	}

	return nil
}

func getRetryTopic(path string) string {
	return fmt.Sprintf("%s%s", basis.RETRY_GROUP_TOPIC_PREFIX, path)
}

func getDLQTopic(path string) string {
	return fmt.Sprintf("%s%s", basis.DLQ_GROUP_TOPIC_PREFIX, path)
}

func parseChannelRemoteAddr(ctx core.Context) string {
	if ctx == nil {
		return ""
	}

	remoteAddr := ctx.RemoteAddr().String()
	if len(remoteAddr) > 0 {
		index := strings.LastIndex(remoteAddr, "/")
		if index >= 0 {
			return remoteAddr[0:index]
		}
	}

	return remoteAddr
}
