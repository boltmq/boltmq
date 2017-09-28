package stgstorelog

import (
	"os"
	"path/filepath"
	"strings"
	"bytes"
	"os/exec"
	"runtime"
)

func GetHome() string {
	if runtime.GOOS == "windows" {
		return GetWindowsHome()
	}

	return GetUnixHome()
}

func GetWindowsHome() string {
	drive := os.Getenv("HOMEDRIVE")
	path := os.Getenv("HOMEPATH")
	home := drive + path

	if drive == "" || path == "" {
		home = os.Getenv("USERPROFILE")
	}

	return home
}

func GetUnixHome() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}

	var stdout bytes.Buffer
	cmd := exec.Command("sh", "-c", "eval echo ~$USER")
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return ""
	}

	result := strings.TrimSpace(stdout.String())

	return result
}

func GetPathSeparator() string {
	return filepath.FromSlash(string(os.PathSeparator))
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func GetParentDirectory(dir string) string {
	return substr(dir, 0, strings.LastIndex(dir, GetPathSeparator()))
}

func substr(s string, pos, length int) string {
	runes := []rune(s)
	size := pos + length
	if size > len(runes) {
		size = len(runes)
	}
	return string(runes[pos:size])
}
