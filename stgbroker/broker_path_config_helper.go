package stgbroker

import "os"

// getTopicConfigPath 获取topic路径
// Author gaoyanlei
// Since 2017/8/21
func GetTopicConfigPath(rootDir string) string {
	return rootDir + string(os.PathSeparator) + "store" + string(os.PathSeparator) + "config" + string(os.PathSeparator) + "topics.json"
}

// getConsumerOffsetPath 获取ConsumerOffset路径
// Author gaoyanlei
// Since 2017/8/21
func GetConsumerOffsetPath(rootDir string) string {
	return rootDir + string(os.PathSeparator) + "store" + string(os.PathSeparator) + "config" + string(os.PathSeparator) + "consumerOffset1.json"
}

// getSubscriptionGroupPath 获取SubscriptionGroup路径
// Author gaoyanlei
// Since 2017/8/21
func GetSubscriptionGroupPath(rootDir string) string {
	return rootDir + string(os.PathSeparator) + "store" + string(os.PathSeparator) + "config" + string(os.PathSeparator) + "subscriptionGroup.json"
}
