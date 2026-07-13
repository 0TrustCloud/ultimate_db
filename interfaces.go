package ultimate_db

import (
	"os"
	"time"
)

// ============================================================================
// 1. Storage & Virtual File System (VFS) Layer
// ============================================================================

// BlockDevice abstracts raw, random-access I/O operations away from the OS file system.
// This allows the buffer pool to write to local files, memory maps, or in-memory test arrays.
type BlockDevice interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	Sync() error
	Stat() (os.FileInfo, error)
	Close() error
}

// ============================================================================
// 2. Memory Management & Cache Eviction
// ============================================================================

// EvictionPolicy manages the page cache access history and decides which unpinned
// frames can be safely recycled when the buffer pool is exhausted.
type EvictionPolicy interface {
	// RecordAccess updates the internal tracking metadata whenever a page is touched.
	RecordAccess(id PageID)
	// Evict selects the next page candidate for recycling based on the underlying strategy.
	Evict() (PageID, bool)
	// Remove explicitly deletes a page from the tracking history (e.g., on page drops).
	Remove(id PageID)
}

// ============================================================================
// 3. Storage Engine Abstractions (KV / Document Layer)
// ============================================================================

// TxnHandle represents a logical snapshot state across isolated operations.
// Updated to expose its ID to lower-level subsystems like the lock manager and recovery loops.
type TxnHandle interface {
	ID() uint64
	Commit() error
	Abort() error
}

// KVStore abstracts your storage engine data operations. This allows full-text
// search indices or execution parsers to sit on top of any backend engine configuration.
type KVStore interface {
	Begin() TxnHandle
	Get(txn TxnHandle, key []byte) ([]byte, error)
	Put(txn TxnHandle, key []byte, value []byte, ttl time.Duration) error
	Delete(txn TxnHandle, key []byte) error
	NewIterator(txn TxnHandle, prefix []byte) KVIterator
	Flush() error
}

// KVIterator provides cursor-based access across ordered structural keys.
type KVIterator interface {
	Next() (key []byte, value []byte, err error)
	Close()
}

// ============================================================================
// 4. Pluggable Data Codecs
// ============================================================================

// Codec standardizes block and record compression layers. Different namespaces
// can wrap different codec implementations depending on the performance/ratio tradeoff required.
type Codec interface {
	// ID returns a unique identification byte stored as a structural prefix flag.
	ID() uint8
	// Encode compresses raw payload slices into compressed bytes.
	Encode(src []byte) ([]byte, error)
	// Decode decompresses compressed payloads back into their original layout.
	Decode(src []byte) ([]byte, error)
}

// ============================================================================
// 5. Production Concurrency Control (Two-Phase Locking)
// ============================================================================

// LockMode defines the access privilege requested by a concurrent transaction.
type LockMode uint8

const (
	LockShared    LockMode = iota // Shared (S) Lock for Read isolation
	LockExclusive                 // Exclusive (X) Lock for Write isolation
)

// LockManager enforces concurrency boundaries across logical keys.
// Implementing this contract protects the engine from race conditions and write skew.
type LockManager interface {
	// Acquire requests an isolation token for a specific transaction handle.
	// It blocks until the lock mode is granted or a deadlock detection timeout fires.
	Acquire(txnID uint64, key string, mode LockMode) error
	
	// Release explicitly drops an active lock held by a completed transaction.
	Release(txnID uint64, key string) error
	
	// ReleaseAll handles bulk transaction unlocks during a Commit or Abort phase (Strict 2PL).
	ReleaseAll(txnID uint64) error
}

// ============================================================================
// 6. Diagnostics, Telemetry, and Observability
// ============================================================================

// EngineMetrics handles zero-allocation event tracking across internal data paths.
// Injecting this interface gives you production visibility into concurrent components.
type EngineMetrics interface {
	IncrBufferPoolHit()
	IncrBufferPoolMiss()
	RecordWalFlushLatency(d time.Duration)
	RecordPageCompactionTime(d time.Duration)
	SetActiveTransactions(count int64)
}
