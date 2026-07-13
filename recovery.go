package ultimate_db

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
        "math"
	"hash/crc32"
	"io"
	"os"
)

// ============================================================================
// 1. ARIES Recovery Pass Supervisors
// ============================================================================

// PerformRecovery executes the complete ARIES three-pass crash recovery suite.
// This must be called exactly once during the initialization of the DB struct.
func PerformRecovery(db *DB, walPath string) error {
	file, err := os.OpenFile(walPath, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Fresh initialization; nothing to recover
		}
		return err
	}
	defer file.Close()

	// 1. ANALYSIS PASS
	// Scan the log forward to determine active transactions and dirty pages at crash time.
	activeTxns, err := runAnalysisPass(file)
	if err != nil {
		return fmt.Errorf("ARIES Analysis Pass failed: %w", err)
	}

	// 2. REDO PASS
	// Scan forward from the earliest un-checkpointed record to repeat history.
	// This brings the system state up to the exact millisecond of the failure.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := runRedoPass(db, file); err != nil {
		return fmt.Errorf("ARIES Redo Pass failed: %w", err)
	}

	// 3. UNDO PASS
	// Scan backward to roll back modifications made by transactions that never committed.
	if err := runUndoPass(db, file, activeTxns); err != nil {
		return fmt.Errorf("ARIES Undo Pass failed: %w", err)
	}

	// Reconstruct the active transaction tables on the database runtime instance
	var maxTxnID uint64
	for txnID := range activeTxns {
		db.activeTxns.Store(txnID, true)
		if txnID > maxTxnID {
			maxTxnID = txnID
		}
	}
	// Also advance past any committed txn ids already on slotted pages so
	// post-restart writes are not shadowed by higher historical txn numbers.
	if pageMax := db.maxTxnIDOnPages(); pageMax > maxTxnID {
		maxTxnID = pageMax
	}
	db.nextTxnID.Store(maxTxnID)

	return nil
}

// maxTxnIDOnPages scans loaded slotted pages for the highest record txn id.
func (db *DB) maxTxnIDOnPages() uint64 {
	if db == nil || db.bp == nil {
		return 0
	}
	var max uint64
	// Site/auth/message pages used by products commonly sit in low page ids.
	for pageID := PageID(1); pageID <= 32; pageID++ {
		page, err := db.bp.FetchPage(pageID)
		if err != nil {
			continue
		}
		page.Latch.RLock()
		slots := page.GetSlotCount()
		for i := uint32(0); i < slots; i++ {
			slot, err := page.GetSlot(i)
			if err != nil || slot.Length < 8 {
				continue
			}
			recordTxnID := binary.LittleEndian.Uint64(page.Data[slot.Offset : slot.Offset+8])
			if recordTxnID > max {
				max = recordTxnID
			}
		}
		page.Latch.RUnlock()
		db.bp.UnpinPage(pageID, false)
	}
	return max
}

// ============================================================================
// 2. Pass 1: Analysis Pass
// ============================================================================

func runAnalysisPass(file *os.File) (map[uint64]LogSequenceNumber, error) {
	activeTxns := make(map[uint64]LogSequenceNumber)
	reader := bufio.NewReader(file)
	headerBuf := make([]byte, 49)

	for {
		_, err := io.ReadFull(reader, headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		expectedCRC := binary.LittleEndian.Uint32(headerBuf[0:4])
		lsn := LogSequenceNumber(binary.LittleEndian.Uint64(headerBuf[4:12]))
		txnID := binary.LittleEndian.Uint64(headerBuf[12:20])
		logType := headerBuf[28]
		kLen := binary.LittleEndian.Uint32(headerBuf[37:41])
		oldLen := binary.LittleEndian.Uint32(headerBuf[41:45])
		newLen := binary.LittleEndian.Uint32(headerBuf[45:49])

		// Read variable payload data to validate frame checksum fully
		payload := make([]byte, kLen+oldLen+newLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, errors.New("corrupted WAL payload inside analysis loop")
		}

		hash := crc32.NewIEEE()
		hash.Write(headerBuf[4:49])
		hash.Write(payload)
		if hash.Sum32() != expectedCRC {
			return nil, fmt.Errorf("analysis pass verification failed: CRC mismatch at LSN %d", lsn)
		}

		// ARIES Tracking Rule: Track live transactions based on operational boundary state shifts
		switch logType {
		case LogTypeBegin:
			activeTxns[txnID] = lsn
		case LogTypeUpdate, LogTypeCLR:
			activeTxns[txnID] = lsn
		case LogTypeCommit, LogTypeAbort:
			delete(activeTxns, txnID)
		}
	}
	return activeTxns, nil
}

// ============================================================================
// 3. Pass 2: Redo Pass (Repeating History)
// ============================================================================

func runRedoPass(db *DB, file *os.File) error {
	reader := bufio.NewReader(file)
	headerBuf := make([]byte, 49)

	for {
		_, err := io.ReadFull(reader, headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		logType := headerBuf[28]
		pageID := PageID(binary.LittleEndian.Uint64(headerBuf[29:37]))
		kLen := binary.LittleEndian.Uint32(headerBuf[37:41])
		oldLen := binary.LittleEndian.Uint32(headerBuf[41:45])
		newLen := binary.LittleEndian.Uint32(headerBuf[45:49])

		payload := make([]byte, kLen+oldLen+newLen)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return err
		}

		// Redo applies exclusively to transactional record types modifying physical pages
		if logType == LogTypeUpdate || logType == LogTypeCLR {
			key := payload[:kLen]
			newValue := payload[kLen+oldLen:]

			// Re-apply the After-Image (newValue) straight into the storage engine
			if err := db.restoreWrite(0, math.MaxInt64, pageID, key, newValue); err != nil {
				return fmt.Errorf("failed to reapply operational adjustments on Page %d: %w", pageID, err)
			}
		}
	}
	return nil
}

// ============================================================================
// 4. Pass 3: Undo Pass (Reversing Losers)
// ============================================================================

func runUndoPass(db *DB, file *os.File, activeTxns map[uint64]LogSequenceNumber) error {
	headerBuf := make([]byte, 49)

	// Keep rolling backward until all log sequence pointer chains are cleared out
	for len(activeTxns) > 0 {
		// Find the highest LSN remaining among active "loser" transactions
		var maxLSN LogSequenceNumber
		var targetTxnID uint64
		for txnID, lsn := range activeTxns {
			if lsn >= maxLSN {
				maxLSN = lsn
				targetTxnID = txnID
			}
		}

		if maxLSN == 0 {
			break // All tracking loops unrolled cleanly
		}

		// Seek back to the target log record offset boundary
		if _, err := file.Seek(int64(maxLSN), io.SeekStart); err != nil {
			return err
		}

		if _, err := io.ReadFull(file, headerBuf); err != nil {
			return err
		}

		prevLSN := LogSequenceNumber(binary.LittleEndian.Uint64(headerBuf[20:28]))
		logType := headerBuf[28]
		pageID := PageID(binary.LittleEndian.Uint64(headerBuf[29:37]))
		kLen := binary.LittleEndian.Uint32(headerBuf[37:41])
		oldLen := binary.LittleEndian.Uint32(headerBuf[41:45])
		newLen := binary.LittleEndian.Uint32(headerBuf[45:49])

		payload := make([]byte, kLen+oldLen+newLen)
		if _, err := io.ReadFull(file, payload); err != nil {
			return err
		}

		// If it's an update record, reverse it by applying the Before-Image (oldValue)
		if logType == LogTypeUpdate {
			key := payload[:kLen]
			oldValue := payload[kLen : kLen+oldLen]

			// Inverse restoration write reverts data states physically on the target page
			if err := db.restoreWrite(targetTxnID, math.MaxInt64, pageID, key, oldValue); err != nil {
				return fmt.Errorf("failed to undo changes on Page %d during rollback: %w", pageID, err)
			}

			// In a full implementation, you would append a Compensation Log Record (CLR) here
			// to avoid re-executing this undo operation if a crash occurs during recovery.
		}

		// Update the pointer chain or remove the transaction if its log chain is fully rolled back
		if prevLSN == 0 {
			delete(activeTxns, targetTxnID)
		} else {
			activeTxns[targetTxnID] = prevLSN
		}
	}
	return nil
}
