package config

import (
	"os"
	"path/filepath"
)

func GetStorePathConsumeQueue(rootDir string) string {
	return rootDir + filepath.FromSlash(string(os.PathSeparator)) + "consumequeue"
}

func GetStorePathIndex(rootDir string) string {
	return rootDir + filepath.FromSlash(string(os.PathSeparator)) + "index"
}

func GetStoreCheckpoint(rootDir string) string {
	return rootDir + filepath.FromSlash(string(os.PathSeparator)) + "checkpoint"
}

func GetAbortFile(rootDir string) string {
	return rootDir + filepath.FromSlash(string(os.PathSeparator)) + "abort"
}

func GetDelayOffsetStorePath(rootDir string) string {
	fileSeparator := filepath.FromSlash(string(os.PathSeparator))
	return rootDir + fileSeparator + "config" + fileSeparator + "delayOffset.json"
}

func GetTranStateTableStorePath(rootDir string) string {
	fileSeparator := filepath.FromSlash(string(os.PathSeparator))
	return rootDir + fileSeparator + "transaction" + fileSeparator + "statetable"
}

func GetTranRedoLogStorePath(rootDir string) string {
	fileSeparator := filepath.FromSlash(string(os.PathSeparator))
	return rootDir + fileSeparator + "transaction" + fileSeparator + "redolog"
}
