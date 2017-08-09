package rebalance

type ConsumerIdsChangeListener interface {
	ConsumerIdsChanged(group string, channels []string)
}
