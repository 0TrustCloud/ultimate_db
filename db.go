package ultimate_db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type DB struct {
	bp         *BufferPool
	wal        *BatchingWAL
	nextTxnID  atomic.Uint64
	activeTxns sync.Map
	quit       chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
	metrics    EngineMetrics
}

func NewDB(bp *BufferPool, wal *BatchingWAL, metrics EngineMetrics) *DB {
	db := &DB{
		bp:      bp,
		wal:     wal,
		quit:    make(chan struct{}),
		metrics: metrics,
	}
	db.startCheckpointer()
	return db
}

func (db *DB) startCheckpointer() {
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				db.bp.FlushAll()
				db.wal.Checkpoint()
			case <-db.quit:
				return
			}
		}
	}()
}

func (db *DB) BeginTxn() uint64 {
	id := db.nextTxnID.Add(1)
	db.activeTxns.Store(id, true)
	if db.metrics != nil {
		db.metrics.SetActiveTransactions(db.countActiveTransactions())
	}
	return id
}

func (db *DB) CommitTxn(txnID uint64) {
	db.activeTxns.Delete(txnID)
	if db.metrics != nil {
		db.metrics.SetActiveTransactions(db.countActiveTransactions())
	}
}

func (db *DB) countActiveTransactions() int64 {
	var count int64
	db.activeTxns.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// SlottedPageSlotCount returns the active slot count for a slotted page.
func (db *DB) SlottedPageSlotCount(pageID PageID) (uint32, error) {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return 0, err
	}
	page.Latch.RLock()
	count := page.GetSlotCount()
	page.Latch.RUnlock()
	db.bp.UnpinPage(pageID, false)
	return count, nil
}

// ResetSlottedPage clears a legacy slotted page after its records are migrated elsewhere.
func (db *DB) ResetSlottedPage(pageID PageID) error {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return err
	}
	page.Latch.Lock()
	page.Init()
	page.Latch.Unlock()
	db.bp.UnpinPage(pageID, true)
	return db.bp.FlushAll()
}

// CompactSlottedPage rebuilds a slotted page, keeping only the latest non-expired record per key.
func (db *DB) CompactSlottedPage(pageID PageID) error {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return err
	}
	page.Latch.Lock()
	db.compactPage(page)
	page.Latch.Unlock()
	db.bp.UnpinPage(pageID, true)
	return nil
}

// compactPage strips out expired versions or deleted slots to defragment slot tables.
func (db *DB) compactPage(page *Page) {
	start := time.Now()
	now := time.Now().UnixNano()
	slotCount := page.GetSlotCount()

	type record struct {
		txnID     uint64
		expiresAt uint64
		key       []byte
		val       []byte
	}
	latestRecords := make(map[string]record)

	// Collect the latest valid active state for each unique string key
	for i := uint32(0); i < slotCount; i++ {
		slot, err := page.GetSlot(i)
		if err != nil || slot.Length == 0 {
			continue // Skip dead slots or structural fragmentation pointers
		}

		recordSlice := page.Data[slot.Offset : slot.Offset+slot.Length]
		recordTxnID := binary.LittleEndian.Uint64(recordSlice[0:8])
		expiresAt := binary.LittleEndian.Uint64(recordSlice[8:16])
		keyLen := binary.LittleEndian.Uint32(recordSlice[16:20])
		valLen := binary.LittleEndian.Uint32(recordSlice[20:24])

		k := recordSlice[24 : 24+keyLen]
		v := recordSlice[24+keyLen : 24+keyLen+valLen]

		existing, exists := latestRecords[string(k)]
		if !exists || recordTxnID > existing.txnID {
			if int64(expiresAt) > now {
				keyCopy := make([]byte, keyLen)
				copy(keyCopy, k)
				valCopy := make([]byte, valLen)
				copy(valCopy, v)
				latestRecords[string(k)] = record{
					txnID:     recordTxnID,
					expiresAt: expiresAt,
					key:       keyCopy,
					val:       valCopy,
				}
			}
		}
	}

	// Completely clean the physical page schema and re-pack slots cleanly
	page.Init()
	var slotIdx uint32 = 0

	for _, rec := range latestRecords {
		recordSize := uint32(RecordHeaderSize + len(rec.key) + len(rec.val))
		upper := page.GetUpperBoundary()
		newOffset := upper - recordSize

		// Safe layout injection copy strings backwards
		targetSlice := page.Data[newOffset:upper]
		binary.LittleEndian.PutUint64(targetSlice[0:8], rec.txnID)
		binary.LittleEndian.PutUint64(targetSlice[8:16], rec.expiresAt)
		binary.LittleEndian.PutUint32(targetSlice[16:20], uint32(len(rec.key)))
		binary.LittleEndian.PutUint32(targetSlice[20:24], uint32(len(rec.val)))
		copy(targetSlice[24:24+len(rec.key)], rec.key)
		copy(targetSlice[24+len(rec.key):], rec.val)

		// Register the fresh clean slot position entries
		page.WriteSlot(slotIdx, Slot{Offset: uint16(newOffset), Length: uint16(recordSize)})
		
		// Move boundaries inward
		page.SetLowerBoundary(PageHeaderSize + ((slotIdx + 1) * 4))
		page.SetUpperBoundary(newOffset)
		slotIdx++
	}
	page.SetSlotCount(slotIdx)

	if db.metrics != nil {
		db.metrics.RecordPageCompactionTime(time.Since(start))
	}
}

// Write transactionalizes mutations over physical page directories matching explicit MVCC schemas.
func (db *DB) Write(pageID PageID, txnID uint64, key []byte, value []byte, mixParam interface{}) error {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return err
	}
	
	page.Latch.Lock()
	defer page.Latch.Unlock()
	defer db.bp.UnpinPage(page.ID, true)

	// 1. Harmonize Time to Live (TTL) parameters with mixed framework values
	nowNano := time.Now().UnixNano()
	var expiresAt uint64 = uint64(nowNano + int64(100*365*24*time.Hour)) // Default far future window

	switch v := mixParam.(type) {
	case time.Duration:
		if v > 0 {
			expiresAt = uint64(time.Now().Add(v).UnixNano())
		}
	case int64:
		if v > 0 {
			expiresAt = uint64(time.Now().Add(time.Duration(v)).UnixNano())
		}
	case int:
		if v > 0 {
			expiresAt = uint64(time.Now().Add(time.Duration(v)).UnixNano())
		}
	}

	// 2. Self-Healing Formatter Guard
	if page.GetUpperBoundary() == 0 || page.GetUpperBoundary() > 32768 {
		page.Init()
	}

	// 3. Structural Boundary Verification & Compaction Pass Triggers
	recordSize := uint32(RecordHeaderSize + len(key) + len(value))
	if !page.IsSafeForInsert(recordSize) {
		db.compactPage(page)
	}
	if !page.IsSafeForInsert(recordSize) {
		return fmt.Errorf("slotted storage page %d overflowed: frame full after complete compression pass", pageID)
	}

	// 4. Data Layout Insertion
	currentUpper := page.GetUpperBoundary()
	newOffset := currentUpper - recordSize

	if newOffset < page.GetLowerBoundary() || newOffset > 32768 {
		return fmt.Errorf("critical slotted calculation pointer anomaly: offset %d corrupts data tables", newOffset)
	}

	targetSlice := page.Data[newOffset:currentUpper]
	binary.LittleEndian.PutUint64(targetSlice[0:8], txnID)
	binary.LittleEndian.PutUint64(targetSlice[8:16], expiresAt)
	binary.LittleEndian.PutUint32(targetSlice[16:20], uint32(len(key)))
	binary.LittleEndian.PutUint32(targetSlice[20:24], uint32(len(value)))
	
	copy(targetSlice[24:24+len(key)], key)
	if value != nil && len(value) > 0 {
		copy(targetSlice[24+len(key):], value)
	}

	// 5. Commit Frame Offsets Natively
	currentSlots := page.GetSlotCount()
	page.WriteSlot(currentSlots, Slot{Offset: uint16(newOffset), Length: uint16(recordSize)})
	
	page.SetLowerBoundary(PageHeaderSize + ((currentSlots + 1) * 4))
	page.SetUpperBoundary(newOffset)
	page.SetSlotCount(currentSlots + 1)

	return nil
}

// Read extracts payloads by searching active slot locations under MVCC rules
// isRecordVisible reports whether a slotted record is visible to readTxnID.
// Committed records (no active txn) remain readable after process restarts even
// when their txn id is higher than the freshly issued read snapshot id.
func isRecordVisible(recordTxnID, readTxnID uint64, isActive bool) bool {
	if isActive {
		return recordTxnID <= readTxnID
	}
	return true
}

func (db *DB) Read(pageID PageID, readTxnID uint64, key []byte) ([]byte, error) {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return nil, err
	}
	defer db.bp.UnpinPage(pageID, false)

	page.Latch.RLock()
	defer page.Latch.RUnlock()

	slotCount := page.GetSlotCount()
	var latestValue []byte
	var highestTxnID uint64 = 0
	var highestSlot int = -1
	now := time.Now().UnixNano()

	// Traverse the slotted registry offsets to construct target states cleanly.
	// Prefer highest slot index among visible records so post-restart writes
	// (low txn ids after empty-WAL recovery) still supersede older high-txn records.
	for i := uint32(0); i < slotCount; i++ {
		slot, err := page.GetSlot(i)
		if err != nil || slot.Length == 0 {
			continue
		}

		recordSlice := page.Data[slot.Offset : slot.Offset+slot.Length]
		recordTxnID := binary.LittleEndian.Uint64(recordSlice[0:8])
		expiresAt := int64(binary.LittleEndian.Uint64(recordSlice[8:16]))
		keyLen := binary.LittleEndian.Uint32(recordSlice[16:20])
		valLen := binary.LittleEndian.Uint32(recordSlice[20:24])

		recordKey := recordSlice[24 : 24+keyLen]
		recordVal := recordSlice[24+keyLen : 24+keyLen+valLen]

		_, isActive := db.activeTxns.Load(recordTxnID)
		isCommitted := !isActive || recordTxnID == readTxnID

		if bytes.Equal(recordKey, key) && isRecordVisible(recordTxnID, readTxnID, isActive) && isCommitted {
			// Later slot wins; tie-break with higher txn id
			better := int(i) > highestSlot || (int(i) == highestSlot && recordTxnID >= highestTxnID)
			if !better {
				continue
			}
			highestSlot = int(i)
			highestTxnID = recordTxnID
			if expiresAt > now {
				latestValue = make([]byte, valLen)
				copy(latestValue, recordVal)
			} else {
				latestValue = nil // Registered structural deletion or timeline visibility expiration
			}
		}
	}

	if latestValue != nil {
		return latestValue, nil
	}
	return nil, errors.New("key not found or expired within slotted scope")
}

func (db *DB) Scan(pageID PageID, readTxnID uint64, prefix []byte, iter func(key, value []byte) bool) error {
	page, err := db.bp.FetchPage(pageID)
	if err != nil { return err }
	defer db.bp.UnpinPage(pageID, false)

	page.Latch.RLock()
	defer page.Latch.RUnlock()

	slotCount := page.GetSlotCount()
	type scanRecord struct {
		txnID uint64
		slot  int
		val   []byte
	}
	latest := make(map[string]scanRecord)
	now := time.Now().UnixNano()

	for i := uint32(0); i < slotCount; i++ {
		slot, err := page.GetSlot(i)
		if err != nil || slot.Length == 0 { continue }

		recordSlice := page.Data[slot.Offset : slot.Offset+slot.Length]
		recordTxnID := binary.LittleEndian.Uint64(recordSlice[0:8])
		expiresAt := int64(binary.LittleEndian.Uint64(recordSlice[8:16]))
		keyLen := binary.LittleEndian.Uint32(recordSlice[16:20])
		valLen := binary.LittleEndian.Uint32(recordSlice[20:24])
		
		recordKey := recordSlice[24 : 24+keyLen]
		recordVal := recordSlice[24+keyLen : 24+keyLen+valLen]
		
		_, isActive := db.activeTxns.Load(recordTxnID)
		if (!isActive || recordTxnID == readTxnID) && bytes.HasPrefix(recordKey, prefix) && isRecordVisible(recordTxnID, readTxnID, isActive) {
			existing, exists := latest[string(recordKey)]
			better := !exists || int(i) > existing.slot || (int(i) == existing.slot && recordTxnID >= existing.txnID)
			if better {
				if expiresAt > now {
					valCopy := make([]byte, valLen)
					copy(valCopy, recordVal)
					latest[string(recordKey)] = scanRecord{recordTxnID, int(i), valCopy}
				} else {
					delete(latest, string(recordKey))
				}
			}
		}
	}

	for k, rec := range latest {
		if !iter([]byte(k), rec.val) { break }
	}
	return nil
}

func (db *DB) WriteCompressed(pageID PageID, txnID uint64, key, value []byte, ttl time.Duration, codec Codec) error {
	compressed, err := codec.Encode(value)
	if err != nil { return fmt.Errorf("codec write failure: %w", err) }
	payload := make([]byte, 1+len(compressed))
	payload[0] = codec.ID()
	copy(payload[1:], compressed)
	return db.Write(pageID, txnID, key, payload, ttl)
}

func (db *DB) ReadCompressed(pageID PageID, readTxnID uint64, key []byte, codec Codec) ([]byte, error) {
	payload, err := db.Read(pageID, readTxnID, key)
	if err != nil { return nil, err }
	if len(payload) < 2 { return nil, errors.New("corrupted layout payload envelope tracking flags") }
	if payload[0] != codec.ID() {
		return nil, fmt.Errorf("mismatched compression codec boundary ID: expected 0x%X, got 0x%X", codec.ID(), payload[0])
	}
	return codec.Decode(payload[1:])
}

func (db *DB) ScanCompressed(pageID PageID, readTxnID uint64, prefix []byte, codec Codec, iter func(key, value []byte) bool) error {
	decompressingIter := func(key, payload []byte) bool {
		if len(payload) < 2 || payload[0] != codec.ID() {
			return iter(key, payload)
		}
		decompressed, err := codec.Decode(payload[1:])
		if err != nil { return false }
		return iter(key, decompressed)
	}
	return db.Scan(pageID, readTxnID, prefix, decompressingIter)
}

// restoreWrite forces static insertions bypassing operational WAL logging streams during ARIES pass processing
func (db *DB) restoreWrite(txnID uint64, expiresAt int64, pageID PageID, key, value []byte) error {
	page, err := db.bp.FetchPage(pageID)
	if err != nil { return err }
	defer db.bp.UnpinPage(pageID, true)

	page.Latch.Lock()
	defer page.Latch.Unlock()

	recordSize := uint32(RecordHeaderSize + len(key) + len(value))
	if !page.IsSafeForInsert(recordSize) {
		db.compactPage(page)
	}
	if !page.IsSafeForInsert(recordSize) {
		return errors.New("page overflow during recovery operations")
	}

	currentUpper := page.GetUpperBoundary()
	newOffset := currentUpper - recordSize
	
	targetSlice := page.Data[newOffset:currentUpper]
	binary.LittleEndian.PutUint64(targetSlice[0:8], txnID)
	binary.LittleEndian.PutUint64(targetSlice[8:16], uint64(expiresAt))
	binary.LittleEndian.PutUint32(targetSlice[16:20], uint32(len(key)))
	binary.LittleEndian.PutUint32(targetSlice[20:24], uint32(len(value)))
	copy(targetSlice[24:24+len(key)], key)
	copy(targetSlice[24+len(key):], value)

	currentSlots := page.GetSlotCount()
	page.WriteSlot(currentSlots, Slot{Offset: uint16(newOffset), Length: uint16(recordSize)})
	
	page.SetLowerBoundary(PageHeaderSize + ((currentSlots + 1) * 4))
	page.SetUpperBoundary(newOffset)
	page.SetSlotCount(currentSlots + 1)

	return nil
}

func (db *DB) HSet(pageID PageID, txnID uint64, hashKey, field, value []byte, ttl time.Duration) error {
	compositeKey := make([]byte, 0, len(PrefixHash)+len(hashKey)+1+len(field))
	compositeKey = append(compositeKey, []byte(PrefixHash)...)
	compositeKey = append(compositeKey, hashKey...)
	compositeKey = append(compositeKey, ':')
	compositeKey = append(compositeKey, field...)
	return db.Write(pageID, txnID, compositeKey, value, ttl)
}

func (db *DB) Close() error {
	var closeErr error
	db.closeOnce.Do(func() {
		close(db.quit)
		db.wg.Wait()
		if err := db.bp.FlushAll(); err != nil {
			closeErr = err
			return
		}
		if err := db.wal.Close(); err != nil {
			closeErr = err
			return
		}
		// Release the OS file handle so deploy/restart and tests can unlink the path.
		if db.bp != nil && db.bp.disk != nil && db.bp.disk.device != nil {
			closeErr = db.bp.disk.device.Close()
		}
	})
	return closeErr
}
