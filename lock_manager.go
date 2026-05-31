package ultimate_db

import (
	"sync"
)

type lockKey struct {
	mode    LockMode
	holders map[uint64]bool
	waiters []chan bool
}

type TwoPLLockManager struct {
	mu    sync.Mutex
	locks map[string]*lockKey
	txns  map[uint64]map[string]bool
}

// New2PLLockManager creates a concurrent strict Two-Phase Locking subsystem handler
func New2PLLockManager() LockManager {
	return &TwoPLLockManager{
		locks: make(map[string]*lockKey),
		txns:  make(map[uint64]map[string]bool),
	}
}

func (m *TwoPLLockManager) Acquire(txnID uint64, key string, mode LockMode) error {
	m.mu.Lock()
	lk, exists := m.locks[key]
	if !exists {
		lk = &lockKey{
			mode:    mode,
			holders: map[uint64]bool{txnID: true},
		}
		m.locks[key] = lk
		m.addTxnLock(txnID, key)
		m.mu.Unlock()
		return nil
	}

	if lk.holders[txnID] {
		if lk.mode == LockExclusive || mode == LockShared {
			m.mu.Unlock()
			return nil
		}
		if len(lk.holders) == 1 {
			lk.mode = LockExclusive
			m.mu.Unlock()
			return nil
		}
	}

	for {
		hasConflict := false
		if lk.mode == LockExclusive || mode == LockExclusive {
			hasConflict = true
		}

		if !hasConflict {
			lk.holders[txnID] = true
			m.addTxnLock(txnID, key)
			m.mu.Unlock()
			return nil
		}

		ch := make(chan bool, 1)
		lk.waiters = append(lk.waiters, ch)
		m.mu.Unlock()

		<-ch // Block the goroutine safely until previous transaction completes execution

		m.mu.Lock()
		lk = m.locks[key]
		if lk == nil {
			lk = &lockKey{
				mode:    mode,
				holders: map[uint64]bool{txnID: true},
			}
			m.locks[key] = lk
			m.addTxnLock(txnID, key)
			m.mu.Unlock()
			return nil
		}
	}
}

func (m *TwoPLLockManager) addTxnLock(txnID uint64, key string) {
	if m.txns[txnID] == nil {
		m.txns[txnID] = make(map[string]bool)
	}
	m.txns[txnID][key] = true
}

func (m *TwoPLLockManager) Release(txnID uint64, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lk, exists := m.locks[key]
	if !exists {
		return nil
	}

	delete(lk.holders, txnID)
	if m.txns[txnID] != nil {
		delete(m.txns[txnID], key)
	}

	if len(lk.holders) == 0 {
		waiters := lk.waiters
		lk.waiters = nil
		delete(m.locks, key)
		for _, w := range waiters {
			w <- true
		}
	}
	return nil
}

func (m *TwoPLLockManager) ReleaseAll(txnID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	keys, exists := m.txns[txnID]
	if !exists {
		return nil
	}

	for key := range keys {
		lk := m.locks[key]
		if lk != nil {
			delete(lk.holders, txnID)
			if len(lk.holders) == 0 {
				waiters := lk.waiters
				lk.waiters = nil
				delete(m.locks, key)
				for _, w := range waiters {
					w <- true
				}
			}
		}
	}
	delete(m.txns, txnID)
	return nil
}
