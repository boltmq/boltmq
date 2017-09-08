package filtersrv

import (
	"fmt"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"os/exec"
)

// FilterServerUtil FilterServer公共方法
// Author rongzhihong
// Since 2017/9/8
type FilterServerUtil struct {
}

// callShell 执行命令
// Author rongzhihong
// Since 2017/9/8
func callShell(shellString string) {
	process := exec.Command(shellString)

	err := process.Start()
	if err != nil {
		logger.Error(fmt.Sprintf("callShell: readLine IOException,%s %s", shellString, err.Error()))
		return
	}

	err = process.Wait()
	if err != nil {
		logger.Error(fmt.Sprintf("callShell: readLine IOException,%s %s", shellString, err.Error()))
		return
	}

	logger.Info("callShell: <%s> OK", shellString)
}
