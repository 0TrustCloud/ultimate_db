package ultimate_db

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

// LegacyKeyMatch decides whether a slotted-page key belongs to the hot btree namespace.
type LegacyKeyMatch func(key []byte) bool

// HotSwapConfig wires a btree primary store with a legacy slotted-page fallback.
type HotSwapConfig struct {
	DB          *DB
	HotRoot     PageID
	LegacyPage  PageID
	MarkerKey   []byte
	LegacyMatch LegacyKeyMatch
	Label       string
}

// HotSwapKVStore reads through legacy slotted storage until migration completes,
// writes only to the hot btree, and lazily promotes legacy hits into btree.
type HotSwapKVStore struct {
	db          *DB
	hot         KVStore
	hotRoot     PageID
	legacyPage  PageID
	markerKey   []byte
	legacyMatch LegacyKeyMatch
	label       string

	migrateOnce sync.Once
}

// NewHotSwapKVStore opens (or creates) a btree-backed store with slotted fallback.
func NewHotSwapKVStore(cfg HotSwapConfig) *HotSwapKVStore {
	if cfg.DB == nil {
		panic("hotswap: database is required")
	}
	if cfg.MarkerKey == nil {
		cfg.MarkerKey = []byte("__hotswap_migrated__")
	}
	return &HotSwapKVStore{
		db:          cfg.DB,
		hot:         NewBTreeKVStoreWithRoot(cfg.DB, cfg.HotRoot),
		hotRoot:     cfg.HotRoot,
		legacyPage:  cfg.LegacyPage,
		markerKey:   cfg.MarkerKey,
		legacyMatch: cfg.LegacyMatch,
		label:       cfg.Label,
	}
}

func (s *HotSwapKVStore) logf(format string, args ...interface{}) {
	tag := s.label
	if tag == "" {
		tag = "hotswap"
	}
	log.Printf("["+tag+"] "+format, args...)
}

func (s *HotSwapKVStore) migrationDone() bool {
	txn := s.hot.Begin()
	_, err := s.hot.Get(txn, s.markerKey)
	_ = txn.Commit()
	return err == nil
}

func (s *HotSwapKVStore) legacyGet(key []byte) ([]byte, bool) {
	if s.db == nil || s.legacyPage == 0 {
		return nil, false
	}
	txn := s.db.BeginTxn()
	defer s.db.CommitTxn(txn)
	if raw, err := s.db.Read(s.legacyPage, txn, key); err == nil && len(raw) > 0 {
		return raw, true
	}
	var found []byte
	_ = s.db.Scan(s.legacyPage, txn, key, func(k, v []byte) bool {
		if bytes.Equal(k, key) && len(v) > 0 {
			found = append([]byte(nil), v...)
		}
		return false
	})
	return found, len(found) > 0
}

func (s *HotSwapKVStore) expireLegacyKey(key []byte) {
	if s.db == nil || s.legacyPage == 0 {
		return
	}
	txn := s.db.BeginTxn()
	_ = s.db.Write(s.legacyPage, txn, key, nil, time.Nanosecond)
	s.db.CommitTxn(txn)
}

func (s *HotSwapKVStore) hotGetNoFallback(key []byte) ([]byte, error) {
	txn := s.hot.Begin()
	raw, err := s.hot.Get(txn, key)
	_ = txn.Commit()
	return raw, err
}

func (s *HotSwapKVStore) promoteLegacy(key, val []byte) {
	if len(val) == 0 {
		return
	}
	txn := s.hot.Begin()
	if err := s.hot.Put(txn, key, val, 0); err != nil {
		_ = txn.Abort()
		return
	}
	_ = txn.Commit()
}

// Begin starts a logical transaction against the hot btree.
func (s *HotSwapKVStore) Begin() TxnHandle { return s.hot.Begin() }

// Get reads from hot btree first, then lazily promotes matching legacy slotted keys.
func (s *HotSwapKVStore) Get(txn TxnHandle, key []byte) ([]byte, error) {
	raw, err := s.hot.Get(txn, key)
	if err == nil && len(raw) > 0 {
		return raw, nil
	}
	if s.migrationDone() {
		return nil, fmt.Errorf("key not found")
	}
	if s.legacyMatch != nil && !s.legacyMatch(key) {
		return nil, fmt.Errorf("key not found")
	}
	legacy, ok := s.legacyGet(key)
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	s.promoteLegacy(key, legacy)
	return legacy, nil
}

// Put always writes to the hot btree.
func (s *HotSwapKVStore) Put(txn TxnHandle, key []byte, value []byte, ttl time.Duration) error {
	return s.hot.Put(txn, key, value, ttl)
}

// Delete removes a key from the hot btree and expires any legacy copy.
func (s *HotSwapKVStore) Delete(txn TxnHandle, key []byte) error {
	if err := s.hot.Delete(txn, key); err != nil {
		return err
	}
	s.expireLegacyKey(key)
	return nil
}

// NewIterator walks hot btree keys and, until migration completes, adds unmatched legacy keys.
func (s *HotSwapKVStore) NewIterator(txn TxnHandle, prefix []byte) KVIterator {
	return &hotSwapIterator{
		store:  s,
		txn:    txn,
		prefix: prefix,
		hot:    s.hot.NewIterator(txn, prefix),
		legacy: map[string][]byte{},
	}
}

// Flush persists btree pages.
func (s *HotSwapKVStore) Flush() error { return s.hot.Flush() }

// MigrateLegacy copies legacy slotted keys into btree, expires migrated slots, and marks complete.
func (s *HotSwapKVStore) MigrateLegacy() (int, error) {
	if s.migrationDone() {
		return 0, nil
	}
	if s.db == nil || s.legacyPage == 0 {
		return 0, nil
	}

	txn := s.db.BeginTxn()
	type entry struct {
		key, val []byte
	}
	var pending []entry
	_ = s.db.Scan(s.legacyPage, txn, nil, func(k, v []byte) bool {
		if len(v) == 0 {
			return true
		}
		if s.legacyMatch != nil && !s.legacyMatch(k) {
			return true
		}
		pending = append(pending, entry{
			key: append([]byte(nil), k...),
			val: append([]byte(nil), v...),
		})
		return true
	})
	s.db.CommitTxn(txn)

	copied := 0
	for _, e := range pending {
		if existing, _ := s.hotGetNoFallback(e.key); len(existing) > 0 {
			s.expireLegacyKey(e.key)
			continue
		}
		putTxn := s.hot.Begin()
		if err := s.hot.Put(putTxn, e.key, e.val, 0); err != nil {
			_ = putTxn.Abort()
			return copied, err
		}
		if err := putTxn.Commit(); err != nil {
			return copied, err
		}
		s.expireLegacyKey(e.key)
		copied++
	}

	if err := s.db.CompactSlottedPage(s.legacyPage); err != nil {
		s.logf("compact legacy page %d: %v", s.legacyPage, err)
	}

	markTxn := s.hot.Begin()
	if err := s.hot.Put(markTxn, s.markerKey, []byte("1"), 0); err != nil {
		_ = markTxn.Abort()
		return copied, err
	}
	if err := markTxn.Commit(); err != nil {
		return copied, err
	}
	if err := s.hot.Flush(); err != nil {
		return copied, err
	}
	s.logf("migrated %d legacy keys from page %d into btree page %d", copied, s.legacyPage, s.hotRoot)
	return copied, nil
}

// StartBackgroundMigration runs MigrateLegacy once in the background.
func (s *HotSwapKVStore) StartBackgroundMigration() {
	s.migrateOnce.Do(func() {
		go func() {
			if n, err := s.MigrateLegacy(); err != nil {
				s.logf("background migration failed after %d keys: %v", n, err)
			} else if n > 0 {
				s.logf("background migration copied %d keys", n)
			}
		}()
	})
}

type hotSwapIterator struct {
	store       *HotSwapKVStore
	txn         TxnHandle
	prefix      []byte
	hot         KVIterator
	legacy      map[string][]byte
	legacyIDs   []string
	legacyPos   int
	legacyReady bool
	doneHot     bool
}

func (it *hotSwapIterator) loadLegacy() {
	if it.legacyReady || it.store.migrationDone() {
		it.legacyReady = true
		return
	}
	it.legacy = map[string][]byte{}
	txn := it.store.db.BeginTxn()
	_ = it.store.db.Scan(it.store.legacyPage, txn, it.prefix, func(k, v []byte) bool {
		if len(v) == 0 {
			return true
		}
		if it.store.legacyMatch != nil && !it.store.legacyMatch(k) {
			return true
		}
		it.legacy[string(k)] = v
		return true
	})
	it.store.db.CommitTxn(txn)
	for k := range it.legacy {
		it.legacyIDs = append(it.legacyIDs, k)
	}
	it.legacyReady = true
}

func (it *hotSwapIterator) Next() ([]byte, []byte, error) {
	if !it.doneHot {
		k, v, err := it.hot.Next()
		if err != io.EOF {
			if err == nil {
				delete(it.legacy, string(k))
			}
			return k, v, err
		}
		it.doneHot = true
		it.loadLegacy()
	}
	for it.legacyPos < len(it.legacyIDs) {
		id := it.legacyIDs[it.legacyPos]
		it.legacyPos++
		val, ok := it.legacy[id]
		if !ok {
			continue
		}
		return []byte(id), val, nil
	}
	return nil, nil, io.EOF
}

func (it *hotSwapIterator) Close() {
	if it.hot != nil {
		it.hot.Close()
	}
}