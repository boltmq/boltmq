// Copyright 2017 luoji

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package server

import (
	"strings"
	"sync"

	"github.com/boltmq/boltmq/broker/rebalance"
	"github.com/boltmq/common/logger"
	"github.com/boltmq/common/message"
	"github.com/boltmq/common/utils/system"
	set "github.com/deckarep/golang-set"
)

// rebalanceManager 平衡锁管理
// Author rongzhihong
// Since 2017/9/20
type rebalanceManager struct {
	mqLockTable *rebalance.MQLockTable
	lock        sync.RWMutex
}

// newRebalanceManager 初始化
// Author rongzhihong
// Since 2017/9/20
func newRebalanceManager() *rebalanceManager {
	rblm := new(rebalanceManager)
	rblm.mqLockTable = rebalance.NewMQLockTable()
	return rblm
}

// isLocked 是否已被锁住
// Author rongzhihong
// Since 2017/9/20
func (rblm *rebalanceManager) isLocked(group string, mq *message.MessageQueue, clientId string) bool {
	rblm.lock.Lock()
	defer rblm.lock.Unlock()

	groupValue := rblm.mqLockTable.Get(group)
	if groupValue != nil {
		lockEntry := groupValue.Get(mq)
		if lockEntry != nil {
			locked := lockEntry.IsLocked(clientId)
			if locked {
				lockEntry.LastUpdateTimestamp = system.CurrentTimeMillis()
			}
			return locked
		}
	}
	return false
}

// tryLock 尝试锁住
// Author rongzhihong
// Since 2017/9/20
func (rblm *rebalanceManager) tryLock(group string, mq *message.MessageQueue, clientId string) bool {
	// 没有被锁住
	if !rblm.isLocked(group, mq, clientId) {
		rblm.lock.Lock()
		defer rblm.lock.Unlock()

		groupValue := rblm.mqLockTable.Get(group)
		if nil == groupValue {
			groupValue = rebalance.NewLockEntryTable()
			rblm.mqLockTable.Put(group, groupValue)
		}

		lockEntry := groupValue.Get(mq)
		if nil == lockEntry {
			lockEntry = rebalance.NewLockEntry()
			lockEntry.ClientId = clientId
			groupValue.Put(mq, lockEntry)
			logger.Infof("tryLock, message queue not locked, I got it. Group: %s New-ClientId: %s %s.", group, clientId, mq)
		}

		if lockEntry.IsLocked(clientId) {
			lockEntry.LastUpdateTimestamp = system.CurrentTimeMillis()
			return true
		}

		oldClientId := lockEntry.ClientId

		// 锁已经过期，抢占它
		if lockEntry.IsExpired() {
			lockEntry.ClientId = clientId
			lockEntry.LastUpdateTimestamp = system.CurrentTimeMillis()

			logger.Warnf("tryLock, message queue lock expired, I got it. Group: %s OldClientId: %s NewClientId: %s %s.",
				group, oldClientId, clientId, mq)
			return true
		}

		// 锁被别的Client占用
		logger.Warnf("tryLock, message queue locked by other client. Group: %s OtherClientId: %s NewClientId: %s %s.",
			group, oldClientId, clientId, mq)
		return false

	}
	return true
}

// tryLockBatch 批量方式锁队列，返回锁定成功的队列集合
// Author rongzhihong
// Since 2017/9/20
func (rblm *rebalanceManager) tryLockBatch(group string, mqs set.Set, clientId string) set.Set {
	lockedMqs := set.NewSet()
	notLockedMqs := set.NewSet()

	// 先通过不加锁的方式尝试查看哪些锁定，哪些没锁定
	for item := range mqs.Iterator().C {
		if mq, ok := item.(*message.MessageQueue); ok {
			if rblm.isLocked(group, mq, clientId) {
				lockedMqs.Add(mq)
			} else {
				notLockedMqs.Add(mq)
			}
		}
	}

	if len(notLockedMqs.ToSlice()) > 0 {
		rblm.lock.Lock()
		defer rblm.lock.Unlock()

		groupValue := rblm.mqLockTable.Get(group)
		if nil == groupValue {
			groupValue = rebalance.NewLockEntryTable()
			rblm.mqLockTable.Put(group, groupValue)
		}

		// 遍历没有锁住的队列
		for notLockMq := range notLockedMqs.Iterator().C {
			if mq, ok := notLockMq.(*message.MessageQueue); ok {
				lockEntry := groupValue.Get(mq)
				if nil == lockEntry {
					lockEntry = rebalance.NewLockEntry()
					lockEntry.ClientId = clientId
					groupValue.Put(mq, lockEntry)

					logger.Infof("tryLockBatch, message queue not locked, I got it. Group: %s NewClientId: %s %s.",
						group, clientId, mq)
				}

				// 已经锁定
				if lockEntry.IsLocked(clientId) {
					lockEntry.LastUpdateTimestamp = system.CurrentTimeMillis()
					lockedMqs.Add(lockEntry)
					continue
				}

				oldClientId := lockEntry.ClientId

				// 锁已经过期，抢占它
				if lockEntry.IsExpired() {
					lockEntry.ClientId = clientId
					lockEntry.LastUpdateTimestamp = system.CurrentTimeMillis()
					logger.Warnf("tryLockBatch, message queue lock expired, I got it. Group: %s OldClientId: %s NewClientId: %s %#v.",
						group, oldClientId, clientId, mq)

					lockedMqs.Add(mq)
					continue
				}

				// 锁被别的Client占用
				logger.Warnf("tryLockBatch, message queue locked by other client. Group: %s OtherClientId: %s NewClientId: %s %#v.",
					group, oldClientId, clientId, mq)
			}
		}
	}
	return lockedMqs
}

// unlockBatch 批量方式解锁队列
// Author rongzhihong
// Since 2017/9/20
func (rblm *rebalanceManager) unlockBatch(group string, mqs set.Set, clientId string) {
	rblm.lock.Lock()
	defer rblm.lock.Unlock()

	groupValue := rblm.mqLockTable.Get(group)
	if nil != groupValue {
		for item := range mqs.Iterator().C {
			if mq, ok := item.(*message.MessageQueue); ok {
				lockEntry := groupValue.Get(mq)

				if nil != lockEntry {
					if strings.EqualFold(lockEntry.ClientId, clientId) {
						groupValue.Remove(mq)
						logger.Infof("unlockBatch, Group: %s %#v %s.", group, mq, clientId)

					} else {
						logger.Warnf("unlockBatch, but mq locked by other client: %s, Group: %s %#v %s.",
							lockEntry.ClientId, group, mq, clientId)
					}
				} else {
					logger.Warnf("unlockBatch, but mq not locked, Group: %s %#v %s.", group, mq, clientId)
				}
			}
		}
	} else {
		logger.Warnf("unlockBatch, group not exist, Group: %s %s.", group, clientId)
	}
}
