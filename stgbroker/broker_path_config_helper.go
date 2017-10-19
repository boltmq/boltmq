package stgbroker

import (
	"os"
)

const (
	configDir = "config"
	separator = string(os.PathSeparator)
)

// GetTopicConfigPath 获取topic.json路径
// Author gaoyanlei
// Since 2017/8/21
func GetTopicConfigPath(rootDir string) string {
	return rootDir + separator + configDir + separator + "topics.json"
}

// GetConsumerOffsetPath 获取consumerOffset.json路径
// Author gaoyanlei
// Since 2017/8/21
func GetConsumerOffsetPath(rootDir string) string {
	return rootDir + separator + configDir + separator + "consumerOffset.json"
}

// GetSubscriptionGroupPath 获取subscriptionGroup.json路径
// Author gaoyanlei
// Since 2017/8/21
func GetSubscriptionGroupPath(rootDir string) string {
	return rootDir + separator + configDir + separator + "subscriptionGroup.json"
}
