package test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strings"
	"testing"
	"git.oschina.net/cloudzone/smartgo/stgbroker"
)

func TestNewClientConfig(t *testing.T) {
	user, err := user.Current()
	if nil == err {
		fmt.Println(user.HomeDir, nil)
	}

	// cross compile support

	if "windows" == runtime.GOOS {
		fmt.Println(homeWindows())
	}

	// Unix-like system, so just assume Unix
	fmt.Println(homeUnix())
}

func TestPathSeparator(t *testing.T) {
	fmt.Println(string(os.PathSeparator))
	user, err := user.Current()
	if nil == err {
	}

	fmt.Println(stgbroker.GetTopicConfigPath(user.HomeDir))
}



func homeUnix() (string, error) {
	// First prefer the HOME environmental variable
	if home := os.Getenv("HOME"); home != "" {
		return home, nil
	}

	// If that fails, try the shell
	var stdout bytes.Buffer
	cmd := exec.Command("sh", "-c", "eval echo ~$USER")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return "", err
	}

	result := strings.TrimSpace(stdout.String())
	if result == "" {
		return "", errors.New("blank output when reading home directory")
	}

	return result, nil
}

func homeWindows() (string, error) {
	drive := os.Getenv("HOMEDRIVE")
	path := os.Getenv("HOMEPATH")
	home := drive + path
	if drive == "" || path == "" {
		home = os.Getenv("USERPROFILE")
	}
	if home == "" {
		return "", errors.New("HOMEDRIVE, HOMEPATH, and USERPROFILE are blank")
	}

	return home, nil
}
