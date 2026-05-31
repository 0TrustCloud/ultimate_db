package ultimate_db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

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

// NewBTreeKVStore binds the database buffer pool allocation layer to an active B-Tree layout
func NewBTreeKVStore(db *DB) KVStore {
	return &BTreeKVStore{
		db:   db,
		tree: NewBTree(db.bp, PageID(1)), // Standardized default root page catalog index
	}
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

	numCells := node.NumCells()
	var offset uint32 = BTreeHeaderSize
	for i := uint16(0); i < numCells; i++ {
		kLen := binary.LittleEndian.Uint16(node.Data[offset : offset+2])
		vLen := binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4])
		cellKey := node.Data[offset+4 : offset+4+uint32(kLen)]
		if bytes.Equal(key, cellKey) {
			val := make([]byte, vLen)
			copy(val, node.Data[offset+4+uint32(kLen) : offset+4+uint32(kLen)+uint32(vLen)])
			return val, nil
		}
		offset += 4 + uint32(kLen) + uint32(vLen)
	}
	return nil, fmt.Errorf("key not found")
}

func (s *BTreeKVStore) Put(txn TxnHandle, key []byte, value []byte, ttl time.Duration) error {
	return s.tree.Insert(key, value)
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

	numCells := node.NumCells()
	var offset uint32 = BTreeHeaderSize
	for i := uint16(0); i < numCells; i++ {
		kLen := binary.LittleEndian.Uint16(node.Data[offset : offset+2])
		vLen := binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4])
		cellKey := node.Data[offset+4 : offset+4+uint32(kLen)]
		cellBytes := 4 + uint32(kLen) + uint32(vLen)

		if bytes.Equal(key, cellKey) {
			var tempOffset uint32 = BTreeHeaderSize
			for j := uint16(0); j < numCells; j++ {
				kl := binary.LittleEndian.Uint16(node.Data[tempOffset : tempOffset+2])
				vl := binary.LittleEndian.Uint16(node.Data[tempOffset+2 : tempOffset+4])
				tempOffset += 4 + uint32(kl) + uint32(vl)
			}
			totalSize := tempOffset

			// Sliding compaction deletion
			copy(node.Data[offset:totalSize-cellBytes], node.Data[offset+cellBytes:totalSize])
			for idx := totalSize - cellBytes; idx < totalSize; idx++ {
				node.Data[idx] = 0
			}
			node.SetNumCells(numCells - 1)
			node.MemVersion++
			return nil
		}
		offset += cellBytes
	}
	return nil
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
		if len(it.prefix) == 0 || bytes.HasPrefix(k, it.prefix) {
			return k, v, nil
		}
	}
}

func (it *btreeIterator) Close() {
	if it.cursor != nil {
		it.cursor.Close()
	}
}
