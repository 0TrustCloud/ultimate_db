package ultimate_db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// IdentityReplicaRootPageID is a dedicated B-tree root for durable identity keys
// (WebAuthn passkeys, OIDC signing material, OIDC clients). It is independent of
// BTreeRootPageID so a primary-tree reset/migration cannot wipe credentials.
const IdentityReplicaRootPageID PageID = 1016

// isDurableIdentityKey reports keys that must survive process restarts and
// primary B-tree recovery edge cases. WebAuthn users live under data:user:*.
func isDurableIdentityKey(key []byte) bool {
	switch {
	case bytes.HasPrefix(key, []byte("data:user:")):
		return true
	case bytes.HasPrefix(key, []byte("data:oidc_")):
		return true
	case bytes.HasPrefix(key, []byte("data:client:")):
		return true
	case bytes.Equal(key, []byte("data:oidc_master_key")):
		return true
	default:
		return false
	}
}

// ReplicatingKVStore dual-writes durable identity keys to a secondary B-tree and
// an optional on-disk JSON mirror. Reads always go to the primary store first,
// with replica / mirror fallback for missing durable keys.
type ReplicatingKVStore struct {
	primary    *BTreeKVStore
	replica    *BTreeKVStore
	mirrorPath string

	mu sync.Mutex
}

// NewReplicatingKVStore wraps primary with an identity replica tree (+ file mirror).
func NewReplicatingKVStore(primary *BTreeKVStore, db *DB) *ReplicatingKVStore {
	if primary == nil {
		panic("kv replica: primary store is required")
	}
	replicaRaw := NewBTreeKVStoreWithRoot(db, IdentityReplicaRootPageID)
	replica, ok := replicaRaw.(*BTreeKVStore)
	if !ok {
		// Should not happen — WithRoot always returns *BTreeKVStore.
		panic("kv replica: unexpected replica store type")
	}
	s := &ReplicatingKVStore{
		primary:    primary,
		replica:    replica,
		mirrorPath: identityMirrorPath(db),
	}
	restored := s.rehydrate()
	if restored > 0 {
		log.Printf("[kv-replica] rehydrated %d durable identity keys (primary↔replica↔mirror)", restored)
	}
	return s
}

func identityMirrorPath(db *DB) string {
	if db == nil || db.bp == nil || db.bp.disk == nil {
		return ""
	}
	dev, ok := db.bp.disk.device.(*OSFileDevice)
	if !ok || dev.path == "" {
		return ""
	}
	return dev.path + ".identity.json"
}

func (s *ReplicatingKVStore) Begin() TxnHandle { return s.primary.Begin() }

func (s *ReplicatingKVStore) Get(txn TxnHandle, key []byte) ([]byte, error) {
	val, err := s.primary.Get(txn, key)
	if err == nil && len(val) > 0 {
		return val, nil
	}
	if !isDurableIdentityKey(key) {
		return nil, err
	}
	// Primary miss for a durable identity key — try replica, then mirror.
	rtxn := s.replica.Begin()
	rVal, rErr := s.replica.Get(rtxn, key)
	_ = rtxn.Commit()
	if rErr == nil && len(rVal) > 0 {
		// Promote back into primary so future reads are hot.
		ptxn := s.primary.Begin()
		_ = s.primary.Put(ptxn, key, rVal, 0)
		_ = ptxn.Commit()
		_ = s.primary.Flush()
		return rVal, nil
	}
	if mVal, ok := s.mirrorGet(key); ok {
		ptxn := s.primary.Begin()
		_ = s.primary.Put(ptxn, key, mVal, 0)
		_ = ptxn.Commit()
		rtxn = s.replica.Begin()
		_ = s.replica.Put(rtxn, key, mVal, 0)
		_ = rtxn.Commit()
		_ = s.primary.Flush()
		_ = s.replica.Flush()
		return mVal, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("key not found")
}

func (s *ReplicatingKVStore) Put(txn TxnHandle, key []byte, value []byte, ttl time.Duration) error {
	if err := s.primary.Put(txn, key, value, ttl); err != nil {
		return err
	}
	if !isDurableIdentityKey(key) || len(value) == 0 {
		return nil
	}
	// Dual-write identity keys into the replica tree (ttl ignored — durable forever).
	rtxn := s.replica.Begin()
	if err := s.replica.Put(rtxn, key, value, 0); err != nil {
		_ = rtxn.Abort()
		return fmt.Errorf("identity replica put: %w", err)
	}
	if err := rtxn.Commit(); err != nil {
		return err
	}
	if err := s.replica.Flush(); err != nil {
		return err
	}
	s.mirrorPut(key, value)
	return nil
}

func (s *ReplicatingKVStore) Delete(txn TxnHandle, key []byte) error {
	err := s.primary.Delete(txn, key)
	if isDurableIdentityKey(key) {
		rtxn := s.replica.Begin()
		_ = s.replica.Delete(rtxn, key)
		_ = rtxn.Commit()
		_ = s.replica.Flush()
		s.mirrorDelete(key)
	}
	return err
}

func (s *ReplicatingKVStore) NewIterator(txn TxnHandle, prefix []byte) KVIterator {
	return s.primary.NewIterator(txn, prefix)
}

func (s *ReplicatingKVStore) Flush() error {
	if err := s.primary.Flush(); err != nil {
		return err
	}
	return s.replica.Flush()
}

// rehydrate syncs durable keys across primary, replica, and file mirror.
// Returns how many keys were written into a store that was missing them.
func (s *ReplicatingKVStore) rehydrate() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect from all three sources.
	merged := map[string][]byte{}
	s.scanDurable(s.primary, merged)
	s.scanDurable(s.replica, merged)
	for k, v := range s.mirrorLoad() {
		if len(v) == 0 {
			continue
		}
		if existing, ok := merged[k]; !ok || len(v) > len(existing) {
			// Prefer longer value (more credentials / richer profile).
			merged[k] = append([]byte(nil), v...)
		}
	}
	if len(merged) == 0 {
		return 0
	}

	restored := 0
	for k, v := range merged {
		key := []byte(k)
		if s.ensurePresent(s.primary, key, v) {
			restored++
		}
		if s.ensurePresent(s.replica, key, v) {
			restored++
		}
	}
	_ = s.primary.Flush()
	_ = s.replica.Flush()
	s.mirrorSave(merged)
	return restored
}

func (s *ReplicatingKVStore) scanDurable(store *BTreeKVStore, out map[string][]byte) {
	if store == nil {
		return
	}
	prefixes := [][]byte{
		[]byte("data:user:"),
		[]byte("data:oidc_"),
		[]byte("data:client:"),
		[]byte("data:oidc_master_key"),
	}
	txn := store.Begin()
	defer func() { _ = txn.Commit() }()
	for _, prefix := range prefixes {
		it := store.NewIterator(txn, prefix)
		for {
			k, v, err := it.Next()
			if err == io.EOF {
				break
			}
			if err != nil || len(v) == 0 {
				break
			}
			if !isDurableIdentityKey(k) {
				continue
			}
			ks := string(k)
			if existing, ok := out[ks]; !ok || len(v) > len(existing) {
				out[ks] = append([]byte(nil), v...)
			}
		}
		it.Close()
	}
}

func (s *ReplicatingKVStore) ensurePresent(store *BTreeKVStore, key, val []byte) bool {
	txn := store.Begin()
	existing, err := store.Get(txn, key)
	_ = txn.Commit()
	if err == nil && len(existing) > 0 {
		// Keep the richer credential blob if primary is stale/smaller.
		if len(existing) >= len(val) {
			return false
		}
	}
	wTxn := store.Begin()
	if err := store.Put(wTxn, key, val, 0); err != nil {
		_ = wTxn.Abort()
		return false
	}
	_ = wTxn.Commit()
	return true
}

// --- JSON file mirror (survives even full page-tree corruption) ---

type identityMirrorFile struct {
	Version    int               `json:"version"`
	UpdatedAt  time.Time         `json:"updated_at"`
	Keys       map[string][]byte `json:"keys"`
}

func (s *ReplicatingKVStore) mirrorLoad() map[string][]byte {
	if s.mirrorPath == "" {
		return nil
	}
	raw, err := os.ReadFile(s.mirrorPath)
	if err != nil || len(raw) == 0 {
		return nil
	}
	var doc identityMirrorFile
	if err := json.Unmarshal(raw, &doc); err != nil {
		log.Printf("[kv-replica] mirror load %s: %v", s.mirrorPath, err)
		return nil
	}
	return doc.Keys
}

func (s *ReplicatingKVStore) mirrorGet(key []byte) ([]byte, bool) {
	m := s.mirrorLoad()
	if m == nil {
		return nil, false
	}
	v, ok := m[string(key)]
	if !ok || len(v) == 0 {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func (s *ReplicatingKVStore) mirrorPut(key, value []byte) {
	if s.mirrorPath == "" || len(value) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.mirrorLoad()
	if m == nil {
		m = map[string][]byte{}
	}
	m[string(key)] = append([]byte(nil), value...)
	s.mirrorSave(m)
}

func (s *ReplicatingKVStore) mirrorDelete(key []byte) {
	if s.mirrorPath == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.mirrorLoad()
	if m == nil {
		return
	}
	delete(m, string(key))
	s.mirrorSave(m)
}

func (s *ReplicatingKVStore) mirrorSave(keys map[string][]byte) {
	if s.mirrorPath == "" {
		return
	}
	doc := identityMirrorFile{
		Version:   1,
		UpdatedAt: time.Now().UTC(),
		Keys:      keys,
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(s.mirrorPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[kv-replica] mirror mkdir: %v", err)
		return
	}
	tmp := s.mirrorPath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Printf("[kv-replica] mirror write: %v", err)
		return
	}
	if err := os.Rename(tmp, s.mirrorPath); err != nil {
		// Windows may need remove-first when target exists.
		_ = os.Remove(s.mirrorPath)
		if err2 := os.Rename(tmp, s.mirrorPath); err2 != nil {
			log.Printf("[kv-replica] mirror rename: %v", err2)
		}
	}
}
