package ultimate_db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

// BTreeRootPageID isolates the key-value backend from the system ORM catalog tables.
const BTreeRootPageID = PageID(1002)

// LegacyKVPageID holds pre-migration slotted/orphaned identity records.
const LegacyKVPageID = PageID(1000)

// LogBTreeRootPageID isolates durable log documents from the full-text search index tree.
const LogBTreeRootPageID = PageID(1001)

// ColdLogBTreeRootPageID stores archived log documents moved out of the hot search path.
const ColdLogBTreeRootPageID = PageID(1003)

// DNSBTreeRootPageID stores authoritative zone records (replaces legacy slotted page 53).
const DNSBTreeRootPageID = PageID(1012)

type BTreeKVStore struct {
	db   *DB
	tree *BTree
}

type btreeTxn struct {
	id uint64
}

func (t *btreeTxn) ID() uint64        { return t.id }
func (t *btreeTxn) Commit() error     { return nil }
func (t *btreeTxn) Abort() error      { return nil }

// NewBTreeKVStore binds the database buffer pool allocation layer to an active isolated B-Tree layout.
// Durable identity keys (WebAuthn data:user:*, OIDC) are dual-written to a replica tree + JSON mirror
// so credentials survive primary-tree resets and deploy restarts.
func NewBTreeKVStore(db *DB) KVStore {
	store := NewBTreeKVStoreWithRoot(db, BTreeRootPageID)
	primary, ok := store.(*BTreeKVStore)
	if !ok {
		return store
	}
	legacyKVMigrated.Do(func() {
		legacy, err := db.bp.FetchPage(LegacyKVPageID)
		if err != nil {
			return
		}
		legacy.Latch.RLock()
		recovered := recoverLegacyKVRecords(db, LegacyKVPageID, legacy.Data[:])
		legacy.Latch.RUnlock()
		db.bp.UnpinPage(LegacyKVPageID, false)
		if imported := importRecoveredKV(primary, recovered); imported > 0 {
			log.Printf("[kv] migrated %d legacy records from page %d into btree page %d", imported, LegacyKVPageID, BTreeRootPageID)
		}
		_ = primary.Flush()
	})
	return NewReplicatingKVStore(primary, db)
}

func btreePageNeedsReset(node *BTreePage) bool {
	pt := node.PageType()
	if pt != PageTypeLeaf && pt != PageTypeInternal {
		return true
	}
	// Corrupt or implausible cell counts indicate a non-B-tree page layout.
	if node.NumCells() > 512 {
		return true
	}
	return false
}

func resetBTreePage(node *BTreePage) {
	node.BTreeInit()
	node.SetPageType(PageTypeLeaf)
	node.SetNumCells(0)
	node.SetNextLeafID(0)
	node.SetParentID(0)
	for i := int(BTreeHeaderSize); i < PageSize; i++ {
		node.Data[i] = 0
	}
}

// ResetBTreeRoot wipes a btree root page to an empty leaf and flushes it.
// Used when leaf chains accumulate duplicate key cells that FindLeaf/Get cannot
// fully address (orphaned next-leaf clones). Callers must re-Put needed keys.
func ResetBTreeRoot(db *DB, rootID PageID) error {
	if db == nil {
		return fmt.Errorf("db required")
	}
	rawPage, err := db.bp.FetchPage(rootID)
	if err != nil {
		rawPage, err = db.bp.NewPage()
		if err != nil {
			return err
		}
		rawPage.ID = rootID
	}
	rawPage.Latch.Lock()
	resetBTreePage(&BTreePage{rawPage})
	rawPage.IsDirty = true
	rawPage.Latch.Unlock()
	db.bp.UnpinPage(rawPage.ID, true)
	return db.bp.FlushAll()
}

var legacyKVMigrated sync.Once

func isKVKeyChar(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == ':', b == '_', b == '-', b == '.', b == '+':
		return true
	default:
		return false
	}
}

func extractJSONObject(raw []byte) ([]byte, int) {
	if len(raw) == 0 || raw[0] != '{' {
		return nil, 0
	}
	depth := 0
	inString := false
	escaped := false
	for i, b := range raw {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if b == '\\' {
				escaped = true
				continue
			}
			if b == '"' {
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return append([]byte(nil), raw[:i+1]...), i + 1
			}
		}
	}
	return nil, 0
}

func recoverKVEntry(out map[string][]byte, key string, value []byte) {
	if key == "" || len(value) == 0 {
		return
	}
	if len(value) > len(out[key]) {
		out[key] = append([]byte(nil), value...)
	}
}

func isRecoverableDataKey(key []byte) bool {
	switch {
	case bytes.HasPrefix(key, []byte("data:user:")):
		return true
	case bytes.HasPrefix(key, []byte("data:session:")):
		return true
	case bytes.HasPrefix(key, []byte("data:oidc_")):
		return true
	case bytes.HasPrefix(key, []byte("data:policy:")):
		return true
	case bytes.HasPrefix(key, []byte("data:mesh_")):
		return true
	case bytes.HasPrefix(key, []byte("data:client:")):
		return true
	// MeshMail durable keys (must survive open/recovery — never ephemeral).
	case bytes.HasPrefix(key, []byte("mail:box:")):
		return true
	case bytes.HasPrefix(key, []byte("mail:msg:")):
		return true
	case bytes.HasPrefix(key, []byte("mail:mbx:")):
		return true
	default:
		return false
	}
}

func recoverSlottedKVFromRaw(raw []byte) map[string][]byte {
	out := make(map[string][]byte)
	now := time.Now().UnixNano()
	for i := 0; i+24 < len(raw); i++ {
		txnID := binary.LittleEndian.Uint64(raw[i : i+8])
		expiresAt := int64(binary.LittleEndian.Uint64(raw[i+8 : i+16]))
		keyLen := binary.LittleEndian.Uint32(raw[i+16 : i+20])
		valLen := binary.LittleEndian.Uint32(raw[i+20 : i+24])
		if txnID == 0 || keyLen == 0 || keyLen > 256 || valLen == 0 || valLen > 16384 {
			continue
		}
		if expiresAt != 0 && expiresAt <= now {
			continue
		}
		end := i + 24 + int(keyLen) + int(valLen)
		if end > len(raw) {
			continue
		}
		key := raw[i+24 : i+24+int(keyLen)]
		if !isRecoverableDataKey(key) {
			continue
		}
		recoverKVEntry(out, string(key), raw[i+24+int(keyLen):end])
	}
	return out
}

func recoverJSONKVFromRaw(raw []byte) map[string][]byte {
	out := make(map[string][]byte)
	marker := []byte("data:")
	for i := 0; i < len(raw)-len(marker); i++ {
		if !bytes.HasPrefix(raw[i:], marker) {
			continue
		}
		j := i + len(marker)
		for j < len(raw) && isKVKeyChar(raw[j]) {
			j++
		}
		key := raw[i:j]
		if j == i+len(marker) || j >= len(raw) || raw[j] != '{' || !isRecoverableDataKey(key) {
			continue
		}
		val, _ := extractJSONObject(raw[j:])
		if val == nil {
			continue
		}
		recoverKVEntry(out, string(key), val)
	}
	return out
}

// recoverLegacyKVRecords imports slotted or orphaned data:* payloads from a page
// before it is reformatted as a B-tree root. The caller must not hold page.Latch.
func recoverLegacyKVRecords(db *DB, pageID PageID, raw []byte) map[string][]byte {
	out := make(map[string][]byte)

	txn := db.BeginTxn()
	_ = db.Scan(pageID, txn, []byte("data:"), func(key, value []byte) bool {
		recoverKVEntry(out, string(key), value)
		return true
	})
	db.CommitTxn(txn)

	for k, v := range recoverSlottedKVFromRaw(raw) {
		recoverKVEntry(out, k, v)
	}
	for k, v := range recoverJSONKVFromRaw(raw) {
		recoverKVEntry(out, k, v)
	}
	return out
}

func importRecoveredKV(store *BTreeKVStore, recovered map[string][]byte) int {
	if len(recovered) == 0 {
		return 0
	}
	imported := 0
	txn := store.Begin()
	for key, value := range recovered {
		existing, err := store.Get(txn, []byte(key))
		if err == nil && len(existing) >= len(value) {
			continue
		}
		if err := store.Put(txn, []byte(key), value, 0); err == nil {
			imported++
		}
	}
	return imported
}

func purgeEphemeralKVKeys(store *BTreeKVStore) int {
	prefixes := [][]byte{
		[]byte("state:grant:"),
		[]byte("state:log:"),
		[]byte("transaction_ledger:"),
		[]byte("state:pop:"),
	}
	txn := store.Begin()
	var toDelete [][]byte
	for _, prefix := range prefixes {
		it := store.NewIterator(txn, prefix)
		for {
			key, _, err := it.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			toDelete = append(toDelete, append([]byte(nil), key...))
		}
		it.Close()
	}
	removed := 0
	for _, key := range toDelete {
		if store.Delete(txn, key) == nil {
			removed++
		}
	}
	return removed
}

// NewBTreeKVStoreWithRoot opens (or initializes) a B-tree rooted at rootID.
func NewBTreeKVStoreWithRoot(db *DB, rootID PageID) KVStore {
	rawPage, err := db.bp.FetchPage(rootID)
	if err != nil {
		rawPage, err = db.bp.NewPage()
		if err != nil {
			panic("Critical: Failed to allocate initial BTree root page: " + err.Error())
		}
		rawPage.ID = rootID
	}

	node := &BTreePage{rawPage}
	// Only reformat pages that are not valid B-tree leaves/internals.
	// Forcing a reset on every open of BTreeRootPageID wiped durable keys
	// (e.g. MeshMail mail:box:*) because recovery only rebuilt a subset of data:*.
	needsReset := btreePageNeedsReset(node)
	var recovered map[string][]byte
	if rootID == LegacyKVPageID || needsReset {
		recovered = recoverLegacyKVRecords(db, rootID, rawPage.Data[:])
	}
	rawPage.Latch.Lock()
	if needsReset {
		resetBTreePage(node)
		rawPage.IsDirty = true
	}
	rawPage.Latch.Unlock()
	db.bp.UnpinPage(rawPage.ID, true)

	store := &BTreeKVStore{
		db:   db,
		tree: NewBTree(db.bp, rootID),
	}
	imported := importRecoveredKV(store, recovered)
	if imported > 0 {
		log.Printf("[kv] recovered %d legacy records into btree page %d", imported, rootID)
	}
	removed := purgeEphemeralKVKeys(store)
	if removed > 0 {
		log.Printf("[kv] purged %d ephemeral SDF records from btree page %d", removed, rootID)
	}
	if needsReset || imported > 0 || removed > 0 {
		_ = store.Flush()
	}
	return store
}

func (s *BTreeKVStore) Flush() error {
	return s.db.bp.FlushAll()
}

func (s *BTreeKVStore) Begin() TxnHandle {
	return &btreeTxn{id: uint64(time.Now().UnixNano())}
}

func (s *BTreeKVStore) Get(txn TxnHandle, key []byte) ([]byte, error) {
	node, err := s.tree.FindLeaf(key)
	if err != nil {
		return nil, err
	}
	defer func() {
		node.Latch.RUnlock()
		s.tree.bp.UnpinPage(node.ID, false)
	}()

	pageSize := uint32(len(node.Data))
	numCells := node.NumCells()
	var offset uint32 = BTreeHeaderSize

	for i := uint16(0); i < numCells; i++ {
		// Hardened Guard: Stop scanning immediately if cell headers overflow the data allocation length
		if offset+4 > pageSize {
			break
		}
		kLen := uint32(binary.LittleEndian.Uint16(node.Data[offset : offset+2]))
		vLen := uint32(binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4]))
		
		// Hardened Guard: Prevent out-of-bounds slicing across corrupt cell sizes
		if offset+4+kLen+vLen > pageSize {
			break
		}

		cellKey := node.Data[offset+4 : offset+4+kLen]
		if bytes.Equal(key, cellKey) {
			val := make([]byte, vLen)
			copy(val, node.Data[offset+4+kLen : offset+4+kLen+vLen])
			return val, nil
		}
		offset += 4 + kLen + vLen
	}
	return nil, fmt.Errorf("key not found")
}

func (s *BTreeKVStore) Put(txn TxnHandle, key []byte, value []byte, ttl time.Duration) error {
	if err := s.tree.Insert(key, value); err != nil {
		return err
	}
	// Durable prefixes must hit disk — Commit() is a no-op on btreeTxn.
	if isDurableIdentityKey(key) ||
		bytes.HasPrefix(key, []byte("data:session:")) ||
		bytes.HasPrefix(key, []byte("mail:")) {
		return s.Flush()
	}
	return nil
}

func (s *BTreeKVStore) Delete(txn TxnHandle, key []byte) error {
	node, err := s.tree.FindLeaf(key)
	if err != nil {
		return err
	}
	version := node.MemVersion
	node.Latch.RUnlock()
	node.Latch.Lock()
	defer node.Latch.Unlock()
	defer s.tree.bp.UnpinPage(node.ID, true)

	if node.MemVersion != version {
		// Version shifted during latch upgrading; proceed with validation scan
	}

	pageSize := uint32(len(node.Data))
	numCells := node.NumCells()
	var offset uint32 = BTreeHeaderSize

	for i := uint16(0); i < numCells; i++ {
		// Hardened Guard: Ensure room exists to safely process cell length indicators
		if offset+4 > pageSize {
			break
		}
		kLen := uint32(binary.LittleEndian.Uint16(node.Data[offset : offset+2]))
		vLen := uint32(binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4]))
		cellBytes := 4 + kLen + vLen

		// Hardened Guard: Stop loop processing if total length maps outside physical slice ranges
		if offset+cellBytes > pageSize {
			break
		}

		cellKey := node.Data[offset+4 : offset+4+kLen]
		if bytes.Equal(key, cellKey) {
			var tempOffset uint32 = BTreeHeaderSize
			for j := uint16(0); j < numCells; j++ {
				if tempOffset+4 > pageSize {
					break
				}
				kl := uint32(binary.LittleEndian.Uint16(node.Data[tempOffset : tempOffset+2]))
				vl := uint32(binary.LittleEndian.Uint16(node.Data[tempOffset+2 : tempOffset+4]))
				if tempOffset+4+kl+vl > pageSize {
					break
				}
				tempOffset += 4 + kl + vl
			}
			totalSize := tempOffset

			// Hardened Slide Boundaries Check
			if offset+cellBytes <= totalSize && totalSize <= pageSize {
				// Sliding compaction deletion
				copy(node.Data[offset:totalSize-cellBytes], node.Data[offset+cellBytes:totalSize])
				for idx := totalSize - cellBytes; idx < totalSize; idx++ {
					node.Data[idx] = 0
				}
				node.SetNumCells(numCells - 1)
				node.MemVersion++
				node.IsDirty = true
				return nil
			}
			// Compaction bounds failed — still mark dirty and report failure.
			return fmt.Errorf("delete compaction failed for key")
		}
		offset += cellBytes
	}
	return fmt.Errorf("key not found")
}

type btreeIterator struct {
	cursor *BTreeCursor
	prefix []byte
}

func (s *BTreeKVStore) NewIterator(txn TxnHandle, prefix []byte) KVIterator {
	cursor, err := NewBTreeCursor(s.tree)
	if err != nil {
		return &btreeIterator{cursor: nil}
	}
	return &btreeIterator{cursor: cursor, prefix: prefix}
}

func (it *btreeIterator) Next() ([]byte, []byte, error) {
	if it.cursor == nil {
		return nil, nil, io.EOF
	}
	for {
		k, v, err := it.cursor.Next()
		if err != nil {
			return nil, nil, err
		}
		if len(it.prefix) == 0 {
			return k, v, nil
		}
		if bytes.HasPrefix(k, it.prefix) {
			return k, v, nil
		}
		// Past the prefix range (keys are sorted) — stop scanning the whole tree.
		if bytes.Compare(k, it.prefix) > 0 {
			return nil, nil, io.EOF
		}
	}
}

func (it *btreeIterator) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
}
