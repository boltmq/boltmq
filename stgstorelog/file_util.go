package stgstorelog

import (
	"os"
	"path/filepath"
	"strings"
)

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
	l := pos + length
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l])
}
