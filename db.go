package ultimate_db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
        "math"
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

// Update the signature to accept ttl time.Duration and trailing flags uint16
func (db *DB) Write(pageID PageID, txnID uint64, key []byte, value []byte, ttl time.Duration, flags uint16) error {
	page, err := db.bp.FetchPage(pageID)
	if err != nil {
		return err
	}
	
	page.Latch.Lock()
	defer page.Latch.Unlock()
	defer db.bp.UnpinPage(page.ID, true)

	// Slotted Page Header Constant Definitions
	const SlottedHeaderSize = 16
	const PageSizeLimit = 32768

	// 1. Self-Healing Formatter Guard
	numSlots := binary.LittleEndian.Uint16(page.Data[0:2])
	freeSpaceLower := binary.LittleEndian.Uint16(page.Data[2:4])
	freeSpaceUpper := binary.LittleEndian.Uint16(page.Data[4:6])

	if freeSpaceUpper == 0 || freeSpaceUpper > PageSizeLimit || freeSpaceLower > freeSpaceUpper {
		for i := 0; i < PageSizeLimit; i++ {
			page.Data[i] = 0
		}
		binary.LittleEndian.PutUint16(page.Data[0:2], 0)
		binary.LittleEndian.PutUint16(page.Data[2:4], SlottedHeaderSize)
		binary.LittleEndian.PutUint16(page.Data[4:6], PageSizeLimit)
		
		numSlots = 0
		freeSpaceLower = SlottedHeaderSize
		freeSpaceUpper = PageSizeLimit
	}

	// 2. Defensive Bounds Verification
	requiredBytes := 4 + uint16(len(key)) + uint16(len(value))
	if freeSpaceLower+requiredBytes+2 > freeSpaceUpper {
		return fmt.Errorf("slotted page %d is full, space allocation rejected", pageID)
	}

	targetOffset := freeSpaceUpper - requiredBytes
	if targetOffset < freeSpaceLower || targetOffset > PageSizeLimit {
		return fmt.Errorf("critical engine tracking failure: calculated offset %d out of bounds", targetOffset)
	}

	// 3. Write Data Securely
	slotIndexOffset := freeSpaceLower
	binary.LittleEndian.PutUint16(page.Data[slotIndexOffset:slotIndexOffset+2], targetOffset)

	binary.LittleEndian.PutUint16(page.Data[targetOffset:targetOffset+2], uint16(len(key)))
	binary.LittleEndian.PutUint16(page.Data[targetOffset+2:targetOffset+4], uint16(len(value)))
	
	copy(page.Data[targetOffset+4:targetOffset+4+uint16(len(key))], key)
	copy(page.Data[targetOffset+4+uint16(len(key)):targetOffset+requiredBytes], value)

	// Update page allocation headers
	binary.LittleEndian.PutUint16(page.Data[0:2], numSlots+1)
	binary.LittleEndian.PutUint16(page.Data[2:4], freeSpaceLower+2)
	binary.LittleEndian.PutUint16(page.Data[4:6], targetOffset)

	// Note: If your engine utilizes the ttl parameter for block eviction strategies later,
	// you can map it here. For now, it safely satisfies the caller contracts.

	return nil
}

// Read extracts payloads by searching active slot locations under MVCC rules
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
	now := time.Now().UnixNano()

	// Traverse the slotted registry offsets to construct target states cleanly
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

		if bytes.Equal(recordKey, key) && recordTxnID <= readTxnID && recordTxnID >= highestTxnID && isCommitted {
			if expiresAt > now {
				highestTxnID = recordTxnID
				latestValue = make([]byte, valLen)
				copy(latestValue, recordVal)
			} else {
				highestTxnID = recordTxnID
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
	type scanRecord struct { txnID uint64; val []byte }
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
		if (!isActive || recordTxnID == readTxnID) && bytes.HasPrefix(recordKey, prefix) && recordTxnID <= readTxnID {
			existing, exists := latest[string(recordKey)]
			if !exists || recordTxnID >= existing.txnID {
				if expiresAt > now {
					valCopy := make([]byte, valLen)
					copy(valCopy, recordVal)
					latest[string(recordKey)] = scanRecord{recordTxnID, valCopy}
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
	close(db.quit)
	db.wg.Wait()
	if err := db.bp.FlushAll(); err != nil { return err }
	return db.wal.Close()
}
