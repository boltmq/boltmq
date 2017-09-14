package filtersrv

import (
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
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
func CallShell(shellString string) {
	defer utils.RecoveredFn()

	process := exec.Command(shellString)
	err := process.Start()
	if err != nil {
		logger.Errorf("callShell: readLine IOException,%s %s", shellString, err.Error())
		return
	}

	err = process.Wait()
	if err != nil {
		logger.Errorf("callShell: readLine IOException,%s %s", shellString, err.Error())
		return
	}

	logger.Infof("callShell: %s OK", shellString)
}
