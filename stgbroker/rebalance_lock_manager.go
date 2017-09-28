package stgbroker

import (
	"git.oschina.net/cloudzone/smartgo/stgbroker/rebalance"
	"git.oschina.net/cloudzone/smartgo/stgcommon/logger"
	"git.oschina.net/cloudzone/smartgo/stgcommon/message"
	"git.oschina.net/cloudzone/smartgo/stgcommon/protocol/body"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils"
	"git.oschina.net/cloudzone/smartgo/stgcommon/utils/timeutil"
	set "github.com/deckarep/golang-set"
	"strings"
	"sync"
)

// RebalanceLockManager 平衡锁管理
// Author rongzhihong
// Since 2017/9/20
type RebalanceLockManager struct {
	ReentrantLock sync.RWMutex
	mqLockTable   *rebalance.MqLockTable
}

// NewRebalanceLockManager 初始化
// Author rongzhihong
// Since 2017/9/20
func NewRebalanceLockManager() *RebalanceLockManager {
	manager := new(RebalanceLockManager)
	manager.mqLockTable = rebalance.NewMqLockTable()
	return manager
}

// isLocked 是否已被锁住
// Author rongzhihong
// Since 2017/9/20
func (manager *RebalanceLockManager) isLocked(group string, mq *message.MessageQueue, clientId string) bool {
	groupValue := manager.mqLockTable.Get(group)
	if groupValue != nil {
		lockEntry := groupValue.Get(mq)
		if lockEntry != nil {
			locked := lockEntry.IsLocked(clientId)
			if locked {
				lockEntry.LastUpdateTimestamp = timeutil.CurrentTimeMillis()
			}
			return locked
		}
	}
	return false
}

// TryLock 尝试锁住
// Author rongzhihong
// Since 2017/9/20
func (manager *RebalanceLockManager) TryLock(group string, mq *message.MessageQueue, clientId string) bool {
	// 没有被锁住
	if !manager.isLocked(group, mq, clientId) {
		manager.ReentrantLock.Lock()
		defer manager.ReentrantLock.Unlock()
		defer utils.RecoveredFn()

		groupValue := manager.mqLockTable.Get(group)
		if nil == groupValue {
			groupValue = rebalance.NewLockEntryTable()
			manager.mqLockTable.Put(group, groupValue)
		}

		lockEntry := groupValue.Get(mq)
		if nil == lockEntry {
			lockEntry = body.NewLockEntry()
			lockEntry.ClientId = clientId
			groupValue.Put(mq, lockEntry)
			logger.Infof("tryLock, message queue not locked, I got it. Group: %s NewClientId: %s %s",
				group,
				clientId,
				mq)
		}

		if lockEntry.IsLocked(clientId) {
			lockEntry.LastUpdateTimestamp = timeutil.CurrentTimeMillis()
			return true
		}

		oldClientId := lockEntry.ClientId

		// 锁已经过期，抢占它
		if lockEntry.IsExpired() {
			lockEntry.ClientId = clientId
			lockEntry.LastUpdateTimestamp = timeutil.CurrentTimeMillis()

			logger.Warnf("tryLock, message queue lock expired, I got it. Group: %s OldClientId: %s NewClientId: %s %v",
				group, oldClientId, clientId, mq)
			return true
		}

		// 锁被别的Client占用
		logger.Warnf("tryLock, message queue locked by other client. Group: %s OtherClientId: %s NewClientId: %s %v",
			group, oldClientId, clientId, mq)
		return false

	} else {
		// 已经锁住，尝试更新时间
		// isLocked 中已经更新了时间，这里不需要再更新
	}

	return true
}

// TryLockBatch 批量方式锁队列，返回锁定成功的队列集合
// Author rongzhihong
// Since 2017/9/20
func (manager *RebalanceLockManager) TryLockBatch(group string, mqs set.Set, clientId string) set.Set {
	lockedMqs := set.NewSet()
	notLockedMqs := set.NewSet()

	// 先通过不加锁的方式尝试查看哪些锁定，哪些没锁定
	for item := range mqs.Iterator().C {
		if mq, ok := item.(*message.MessageQueue); ok {
			if manager.isLocked(group, mq, clientId) {
				lockedMqs.Add(mq)
			} else {
				notLockedMqs.Add(mq)
			}
		}
	}

	if len(notLockedMqs.ToSlice()) > 0 {
		manager.ReentrantLock.Lock()
		defer manager.ReentrantLock.Unlock()
		defer utils.RecoveredFn()

		groupValue := manager.mqLockTable.Get(group)
		if nil == groupValue {
			groupValue = rebalance.NewLockEntryTable()
			manager.mqLockTable.Put(group, groupValue)
		}

		// 遍历没有锁住的队列
		for notLockMq := range notLockedMqs.Iterator().C {
			if mq, ok := notLockMq.(*message.MessageQueue); ok {
				lockEntry := groupValue.Get(mq)
				if nil == lockEntry {
					lockEntry = body.NewLockEntry()
					lockEntry.ClientId = clientId
					groupValue.Put(mq, lockEntry)

					logger.Infof("tryLockBatch, message queue not locked, I got it. Group: %s NewClientId: %s %#v",
						group, clientId, mq)
				}

				// 已经锁定
				if lockEntry.IsLocked(clientId) {
					lockEntry.LastUpdateTimestamp = timeutil.CurrentTimeMillis()
					lockedMqs.Add(lockEntry)
					continue
				}

				oldClientId := lockEntry.ClientId

				// 锁已经过期，抢占它
				if lockEntry.IsExpired() {
					lockEntry.ClientId = clientId
					lockEntry.LastUpdateTimestamp = timeutil.CurrentTimeMillis()
					logger.Warnf("tryLockBatch, message queue lock expired, I got it. Group: %s OldClientId: %s NewClientId: %s %#v",
						group, oldClientId, clientId, mq)

					lockedMqs.Add(mq)
					continue
				}

				// 锁被别的Client占用
				logger.Warnf("tryLockBatch, message queue locked by other client. Group: %s OtherClientId: %s NewClientId: %s %#v",
					group, oldClientId, clientId, mq)
			}
		}
	}
	return lockedMqs
}

// UnlockBatch 批量方式解锁队列
// Author rongzhihong
// Since 2017/9/20
func (manager *RebalanceLockManager) UnlockBatch(group string, mqs set.Set, clientId string) {
	manager.ReentrantLock.Lock()
	defer manager.ReentrantLock.Unlock()
	defer utils.RecoveredFn()

	groupValue := manager.mqLockTable.Get(group)
	if nil != groupValue {
		for item := range mqs.Iterator().C {
			if mq, ok := item.(*message.MessageQueue); ok {
				lockEntry := groupValue.Get(mq)

				if nil != lockEntry {
					if strings.EqualFold(lockEntry.ClientId, clientId) {
						groupValue.Remove(mq)
						logger.Infof("unlockBatch, Group: %s %#v %s", group, mq, clientId)

					} else {
						logger.Warnf("unlockBatch, but mq locked by other client: %s, Group: %s %#v %s",
							lockEntry.ClientId, group, mq, clientId)
					}
				} else {
					logger.Warnf("unlockBatch, but mq not locked, Group: %s %#v %s", group, mq, clientId)
				}
			}
		}
	} else {
		logger.Warnf("unlockBatch, group not exist, Group: %s %s", group, clientId)
	}
}
