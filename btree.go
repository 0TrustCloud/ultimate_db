package ultimate_db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

var ErrPageFull = errors.New("page requires splitting")

type BTree struct {
	bp     *BufferPool
	rootID PageID
}

type BTreePage struct{ *Page }

func NewBTree(bp *BufferPool, rootID PageID) *BTree {
	return &BTree{bp: bp, rootID: rootID}
}

func (p *BTreePage) BTreeInit() {
	for i := 0; i < int(BTreeHeaderSize); i++ {
		p.Data[i] = 0
	}
}

func (p *BTreePage) PageType() uint16           { return binary.LittleEndian.Uint16(p.Data[0:2]) }
func (p *BTreePage) SetPageType(t uint16)       { binary.LittleEndian.PutUint16(p.Data[0:2], t) }
func (p *BTreePage) NumCells() uint16           { return binary.LittleEndian.Uint16(p.Data[2:4]) }
func (p *BTreePage) SetNumCells(n uint16)       { binary.LittleEndian.PutUint16(p.Data[2:4], n) }
func (p *BTreePage) NextLeafID() PageID         { return PageID(binary.LittleEndian.Uint64(p.Data[4:12])) }
func (p *BTreePage) SetNextLeafID(id PageID)    { binary.LittleEndian.PutUint64(p.Data[4:12], uint64(id)) }
func (p *BTreePage) ParentID() PageID           { return PageID(binary.LittleEndian.Uint64(p.Data[12:20])) }
func (p *BTreePage) SetParentID(id PageID)      { binary.LittleEndian.PutUint64(p.Data[12:20], uint64(id)) }
func (p *BTreePage) RightmostChildID() PageID   { return PageID(binary.LittleEndian.Uint32(p.Data[20:24])) }
func (p *BTreePage) SetRightmostChildID(id PageID) { binary.LittleEndian.PutUint32(p.Data[20:24], uint32(id)) }

func (p *BTreePage) IsSafeForInsert(requiredBytes uint32) bool {
	numCells := p.NumCells()
	var offset uint32 = BTreeHeaderSize
	for i := uint16(0); i < numCells; i++ {
		if p.PageType() == PageTypeLeaf {
			if offset+4 > PageSize {
				return false
			}
			kLen := uint32(binary.LittleEndian.Uint16(p.Data[offset : offset+2]))
			vLen := uint32(binary.LittleEndian.Uint16(p.Data[offset+2 : offset+4]))
			if offset+4+kLen+vLen > PageSize {
				return false
			}
			offset += 4 + kLen + vLen
		} else {
			if offset+10 > PageSize {
				return false
			}
			kLen := uint32(binary.LittleEndian.Uint16(p.Data[offset : offset+2]))
			if offset+10+kLen > PageSize {
				return false
			}
			offset += 10 + kLen
		}
	}
	return (offset + requiredBytes) < PageSize
}

type BTreeCursor struct {
	tree     *BTree
	currNode *BTreePage
	cellIdx  uint16
	offset   uint32
	isEOF    bool
}

func NewBTreeCursor(tree *BTree) (*BTreeCursor, error) {
	currID := tree.rootID
	for {
		raw, err := tree.bp.FetchPage(currID)
		if err != nil {
			return nil, err
		}
		raw.Latch.RLock()
		node := &BTreePage{raw}

		if node.PageType() == PageTypeLeaf {
			return &BTreeCursor{
				tree:     tree,
				currNode: node,
				cellIdx:  0,
				offset:   BTreeHeaderSize,
				isEOF:    node.NumCells() == 0,
			}, nil
		}

		if node.NumCells() == 0 || BTreeHeaderSize+10 > PageSize {
			node.Latch.RUnlock()
			tree.bp.UnpinPage(currID, false)
			return nil, errors.New("corrupt internal nodes during cursor initialization")
		}

		childID := PageID(binary.LittleEndian.Uint64(node.Data[BTreeHeaderSize+2 : BTreeHeaderSize+10]))
		node.Latch.RUnlock()
		tree.bp.UnpinPage(currID, false)
		currID = childID
	}
}

func (c *BTreeCursor) Next() ([]byte, []byte, error) {
	if c.isEOF {
		return nil, nil, io.EOF
	}

	if c.offset+4 > PageSize {
		return nil, nil, errors.New("cursor out of bounds of active block layout parsing window")
	}

	kLen := binary.LittleEndian.Uint16(c.currNode.Data[c.offset : c.offset+2])
	vLen := binary.LittleEndian.Uint16(c.currNode.Data[c.offset+2 : c.offset+4])
	
	if c.offset+4+uint32(kLen)+uint32(vLen) > PageSize {
		return nil, nil, errors.New("corrupt frame cell sizing layout parameters detected")
	}

	key := make([]byte, kLen)
	val := make([]byte, vLen)
	copy(key, c.currNode.Data[c.offset+4 : c.offset+4+uint32(kLen)])
	copy(val, c.currNode.Data[c.offset+4+uint32(kLen) : c.offset+4+uint32(kLen)+uint32(vLen)])

	c.cellIdx++
	c.offset += 4 + uint32(kLen) + uint32(vLen)

	if c.cellIdx >= c.currNode.NumCells() {
		nextLeafID := c.currNode.NextLeafID()
		c.currNode.Latch.RUnlock()
		c.tree.bp.UnpinPage(c.currNode.ID, false)

		if nextLeafID == 0 {
			c.isEOF = true
			c.currNode = nil
		} else {
			nextRaw, err := c.tree.bp.FetchPage(nextLeafID)
			if err != nil {
				return nil, nil, err
			}
			nextRaw.Latch.RLock()
			c.currNode = &BTreePage{nextRaw}
			c.cellIdx = 0
			c.offset = BTreeHeaderSize
		}
	}
	return key, val, nil
}

func (c *BTreeCursor) Close() {
	if c.currNode != nil {
		c.currNode.Latch.RUnlock()
		c.tree.bp.UnpinPage(c.currNode.ID, false)
		c.currNode = nil
		c.isEOF = true
	}
}

type JoinResult struct {
	Key        []byte
	LeftValue  []byte
	RightValue []byte
}

func SortMergeJoin(leftTree, rightTree *BTree) ([]JoinResult, error) {
	leftCursor, err := NewBTreeCursor(leftTree)
	if err != nil {
		return nil, err
	}
	defer leftCursor.Close()

	rightCursor, err := NewBTreeCursor(rightTree)
	if err != nil {
		return nil, err
	}
	defer rightCursor.Close()

	var results []JoinResult
	lKey, lVal, lErr := leftCursor.Next()
	rKey, rVal, rErr := rightCursor.Next()

	for lErr == nil && rErr == nil {
		cmp := bytes.Compare(lKey, rKey)
		if cmp == 0 {
			results = append(results, JoinResult{Key: lKey, LeftValue: lVal, RightValue: rVal})
			lKey, lVal, lErr = leftCursor.Next()
			rKey, rVal, rErr = rightCursor.Next()
		} else if cmp < 0 {
			lKey, lVal, lErr = leftCursor.Next()
		} else {
			rKey, rVal, rErr = rightCursor.Next()
		}
	}

	if lErr != nil && lErr != io.EOF {
		return nil, lErr
	}
	if rErr != nil && rErr != io.EOF {
		return nil, rErr
	}
	return results, nil
}

func (tree *BTree) Scan(prefix string) ([][]byte, [][]byte, error) {
	var keys, values [][]byte
	currNode, err := tree.FindLeaf([]byte(prefix))
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		currNode.Latch.RUnlock()
		tree.bp.UnpinPage(currNode.ID, false)
	}()

	for {
		numCells := currNode.NumCells()
		offset := uint32(BTreeHeaderSize)

		for i := uint16(0); i < numCells; i++ {
			if offset+4 > PageSize {
				break
			}
			kLen := binary.LittleEndian.Uint16(currNode.Data[offset : offset+2])
			vLen := binary.LittleEndian.Uint16(currNode.Data[offset+2 : offset+4])
			
			if offset+4+uint32(kLen)+uint32(vLen) > PageSize {
				break
			}

			key := currNode.Data[offset+4 : offset+4+uint32(kLen)]
			val := currNode.Data[offset+4+uint32(kLen) : offset+4+uint32(kLen)+uint32(vLen)]

			if strings.HasPrefix(string(key), prefix) {
				keyCopy, valCopy := make([]byte, len(key)), make([]byte, len(val))
				copy(keyCopy, key)
				copy(valCopy, val)
				keys = append(keys, keyCopy)
				values = append(values, valCopy)
			}
			offset += 4 + uint32(kLen) + uint32(vLen)
		}

		nextID := currNode.NextLeafID()
		if nextID == 0 {
			break
		}

		currNode.Latch.RUnlock()
		tree.bp.UnpinPage(currNode.ID, false)
		rawPage, err := tree.bp.FetchPage(nextID)
		if err != nil {
			return keys, values, err
		}
		rawPage.Latch.RLock()
		currNode = &BTreePage{rawPage}
	}
	return keys, values, nil
}

func (tree *BTree) FindLeaf(key []byte) (*BTreePage, error) {
	currID := tree.rootID
	currRaw, err := tree.bp.FetchPage(currID)
	if err != nil {
		return nil, err
	}
	currRaw.Latch.RLock()
	currNode := &BTreePage{currRaw}

	for currNode.PageType() == PageTypeInternal {
		childID := tree.findChildInInternalNode(currNode, key)
		childRaw, err := tree.bp.FetchPage(childID)
		if err != nil {
			currNode.Latch.RUnlock()
			tree.bp.UnpinPage(currID, false)
			return nil, err
		}
		childRaw.Latch.RLock()
		currNode.Latch.RUnlock()
		tree.bp.UnpinPage(currID, false)
		currID = childID
		currNode = &BTreePage{childRaw}
	}
	return currNode, nil
}

func (tree *BTree) Insert(key, value []byte) error {
	reqBytes := uint32(4 + len(key) + len(value))
	currID := tree.rootID
	currRaw, err := tree.bp.FetchPage(currID)
	if err != nil {
		return err
	}
	currRaw.Latch.RLock()
	currNode := &BTreePage{currRaw}

	for currNode.PageType() == PageTypeInternal {
		childID := tree.findChildInInternalNode(currNode, key)
		childRaw, err := tree.bp.FetchPage(childID)
		if err != nil {
			currRaw.Latch.RUnlock()
			tree.bp.UnpinPage(currID, false)
			return err
		}
		childRaw.Latch.RLock()
		currRaw.Latch.RUnlock()
		tree.bp.UnpinPage(currID, false)
		currID = childID
		currRaw = childRaw
		currNode = &BTreePage{currRaw}
	}

	if currNode.IsSafeForInsert(reqBytes) {
		version := currRaw.MemVersion
		currRaw.Latch.RUnlock()
		currRaw.Latch.Lock()
		if currRaw.MemVersion != version {
			currRaw.Latch.Unlock()
			tree.bp.UnpinPage(currID, false)
			return tree.pessimisticInsert(key, value)
		}
		if currNode.IsSafeForInsert(reqBytes) {
			err := tree.insertIntoLeaf(currNode, key, value)
			currRaw.Latch.Unlock()
			tree.bp.UnpinPage(currID, true)
			return err
		}
		currRaw.Latch.Unlock()
	} else {
		currRaw.Latch.RUnlock()
	}

	tree.bp.UnpinPage(currID, false)
	return tree.pessimisticInsert(key, value)
}

func (tree *BTree) pessimisticInsert(key, value []byte) error {
	currID := tree.rootID
	var lockedAncestors []*Page
	currRaw, err := tree.bp.FetchPage(currID)
	if err != nil {
		return err
	}
	currRaw.Latch.Lock()
	lockedAncestors = append(lockedAncestors, currRaw)
	currNode := &BTreePage{currRaw}
	reqBytes := uint32(4 + len(key) + len(value))

	for currNode.PageType() == PageTypeInternal {
		childID := tree.findChildInInternalNode(currNode, key)
		childRaw, err := tree.bp.FetchPage(childID)
		if err != nil {
			tree.releaseAncestors(lockedAncestors)
			return err
		}
		childRaw.Latch.Lock()
		childNode := &BTreePage{childRaw}
		if childNode.IsSafeForInsert(reqBytes) {
			tree.releaseAncestors(lockedAncestors)
			lockedAncestors = []*Page{}
		}
		lockedAncestors = append(lockedAncestors, childRaw)
		currID = childID
		currNode = childNode
	}

	err = tree.insertIntoLeaf(currNode, key, value)
	if err != nil && errors.Is(err, ErrPageFull) {
		err = tree.SplitLeaf(currNode, lockedAncestors)
	}
	tree.releaseAncestors(lockedAncestors)
	return err
}

func (tree *BTree) releaseAncestors(ancestors []*Page) {
	for i := len(ancestors) - 1; i >= 0; i-- {
		ancestors[i].Latch.Unlock()
		tree.bp.UnpinPage(ancestors[i].ID, true)
	}
}

func (tree *BTree) SplitLeaf(node *BTreePage, lockedAncestors []*Page) error {
	newRawPage, err := tree.bp.NewPage()
	if err != nil {
		return err
	}
	newRawPage.Latch.Lock()
	defer newRawPage.Latch.Unlock()
	defer tree.bp.UnpinPage(newRawPage.ID, true)
	
	newLeaf := &BTreePage{newRawPage}
	newLeaf.BTreeInit()
	newLeaf.SetPageType(PageTypeLeaf)
	newLeaf.SetNextLeafID(node.NextLeafID())
	node.SetNextLeafID(newLeaf.ID)
	newLeaf.SetParentID(node.ParentID())

	numCells := node.NumCells()
	midPoint := numCells / 2
	var offset uint32 = BTreeHeaderSize
	var midKey []byte
	
	for i := uint16(0); i < numCells; i++ {
		if offset+4 > PageSize {
			break
		}
		kLen := uint32(binary.LittleEndian.Uint16(node.Data[offset : offset+2]))
		vLen := uint32(binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4]))
		
		if offset+4+kLen+vLen > PageSize {
			break
		}

		if i == midPoint {
			midKey = make([]byte, kLen)
			copy(midKey, node.Data[offset+4 : offset+4+kLen])
			break
		}
		offset += 4 + kLen + vLen
	}

	if offset > PageSize {
		offset = PageSize
	}

	bytesToMove := uint32(PageSize) - offset
	if bytesToMove > 0 && offset+bytesToMove <= PageSize {
		copy(newLeaf.Data[BTreeHeaderSize:], node.Data[offset:offset+bytesToMove])
	}

	newLeaf.SetNumCells(numCells - midPoint)
	node.SetNumCells(midPoint)
	
	for i := offset; i < PageSize; i++ {
		node.Data[i] = 0
	}
	
	node.MemVersion++
	newLeaf.MemVersion++
	return tree.promoteToParent(node.ID, newLeaf.ID, midKey, lockedAncestors)
}

func (tree *BTree) SplitInternalNode(node *BTreePage, lockedAncestors []*Page) error {
	newRawPage, err := tree.bp.NewPage()
	if err != nil {
		return err
	}
	newRawPage.Latch.Lock()
	defer newRawPage.Latch.Unlock()
	defer tree.bp.UnpinPage(newRawPage.ID, true)
	
	newInternal := &BTreePage{newRawPage}
	newInternal.BTreeInit()
	newInternal.SetPageType(PageTypeInternal)
	newInternal.SetParentID(node.ParentID())

	numCells := node.NumCells()
	midPoint := numCells / 2
	var offset uint32 = BTreeHeaderSize
	var pivotKey []byte
	var midCellLeftChild PageID
	var midCellEnd uint32
	
	for i := uint16(0); i < numCells; i++ {
		if offset+10 > PageSize {
			break
		}
		kLen := uint32(binary.LittleEndian.Uint16(node.Data[offset : offset+2]))
		
		if offset+10+kLen > PageSize {
			break
		}

		if i == midPoint {
			midCellLeftChild = PageID(binary.LittleEndian.Uint64(node.Data[offset+2 : offset+10]))
			pivotKey = make([]byte, kLen)
			copy(pivotKey, node.Data[offset+10 : offset+10+kLen])
			midCellEnd = offset + 10 + kLen
			break
		}
		offset += 10 + kLen
	}

	if midCellEnd == 0 || midCellEnd > PageSize {
		midCellEnd = offset
	}
	if midCellEnd > PageSize {
		midCellEnd = PageSize
	}

	newInternal.SetRightmostChildID(node.RightmostChildID())
	node.SetRightmostChildID(midCellLeftChild)
	
	bytesToMove := uint32(PageSize) - midCellEnd
	if bytesToMove > 0 && midCellEnd+bytesToMove <= PageSize {
		copy(newInternal.Data[BTreeHeaderSize:], node.Data[midCellEnd : midCellEnd+bytesToMove])
	}

	newInternal.SetNumCells(numCells - midPoint - 1)
	node.SetNumCells(midPoint)
	
	for i := midCellEnd; i < PageSize; i++ {
		node.Data[i] = 0
	}
	
	node.MemVersion++
	newInternal.MemVersion++
	return tree.promoteToParent(node.ID, newInternal.ID, pivotKey, lockedAncestors)
}

func (tree *BTree) promoteToParent(leftChildID, rightChildID PageID, pivotKey []byte, lockedAncestors []*Page) error {
	if leftChildID == tree.rootID {
		newRootRaw, err := tree.bp.NewPage()
		if err != nil {
			return err
		}
		newRootRaw.Latch.Lock()
		defer newRootRaw.Latch.Unlock()
		defer tree.bp.UnpinPage(newRootRaw.ID, true)
		
		newRoot := &BTreePage{newRootRaw}
		newRoot.BTreeInit()
		newRoot.SetPageType(PageTypeInternal)
		newRoot.SetNumCells(1)
		
		offset := BTreeHeaderSize
		binary.LittleEndian.PutUint16(newRoot.Data[offset:offset+2], uint16(len(pivotKey)))
		binary.LittleEndian.PutUint64(newRoot.Data[offset+2:offset+10], uint64(leftChildID))
		copy(newRoot.Data[offset+10:], pivotKey)
		newRoot.SetRightmostChildID(rightChildID)
		newRoot.MemVersion++
		tree.rootID = newRoot.ID
		return nil
	}
	
	parentRaw := lockedAncestors[len(lockedAncestors)-2]
	parentNode := &BTreePage{parentRaw}
	err := tree.insertIntoInternal(parentNode, pivotKey, leftChildID, rightChildID)
	if err != nil && errors.Is(err, ErrPageFull) {
		return tree.SplitInternalNode(parentNode, lockedAncestors[:len(lockedAncestors)-1])
	}
	return err
}

func (tree *BTree) findChildInInternalNode(node *BTreePage, searchKey []byte) PageID {
	numCells := node.NumCells()
	var offset uint32 = BTreeHeaderSize
	for i := uint16(0); i < numCells; i++ {
		if offset+10 > PageSize {
			break
		}
		kLen := uint32(binary.LittleEndian.Uint16(node.Data[offset : offset+2]))
		if offset+10+kLen > PageSize {
			break
		}
		
		childID := PageID(binary.LittleEndian.Uint64(node.Data[offset+2 : offset+10]))
		cellKey := node.Data[offset+10 : offset+10+kLen]
		if bytes.Compare(searchKey, cellKey) < 0 {
			return childID
		}
		offset += 10 + kLen
	}
	return node.RightmostChildID()
}

func (tree *BTree) insertIntoLeaf(node *BTreePage, newKey, newVal []byte) error {
	reqBytes := uint32(4 + len(newKey) + len(newVal))

	// Purge every existing cell with this key first. A prior migration/clone bug
	// could leave duplicate leaf cells; replacing only the first left the rest
	// and made List/Iterator report the same ETL workflow dozens of times.
	for {
		numCells := node.NumCells()
		offset := uint32(BTreeHeaderSize)
		found := false
		for i := uint16(0); i < numCells; i++ {
			if offset+4 > PageSize {
				break
			}
			kLen := uint32(binary.LittleEndian.Uint16(node.Data[offset : offset+2]))
			vLen := uint32(binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4]))
			cellBytes := 4 + kLen + vLen
			if offset+cellBytes > PageSize {
				break
			}
			cellKey := node.Data[offset+4 : offset+4+kLen]
			if bytes.Equal(newKey, cellKey) {
				if err := tree.deleteLeafCell(node, offset, cellBytes); err != nil {
					return err
				}
				found = true
				break // restart scan after compacting slide
			}
			offset += cellBytes
		}
		if !found {
			break
		}
	}

	if !node.IsSafeForInsert(reqBytes) {
		return ErrPageFull
	}

	numCells := node.NumCells()
	insertOffset := uint32(0)
	found := false
	offset := uint32(BTreeHeaderSize)
	for i := uint16(0); i < numCells; i++ {
		if offset+4 > PageSize {
			break
		}
		kLen := uint32(binary.LittleEndian.Uint16(node.Data[offset : offset+2]))
		vLen := uint32(binary.LittleEndian.Uint16(node.Data[offset+2 : offset+4]))
		cellBytes := 4 + kLen + vLen
		if offset+cellBytes > PageSize {
			break
		}

		cellKey := node.Data[offset+4 : offset+4+kLen]
		if !found && bytes.Compare(newKey, cellKey) < 0 {
			insertOffset = offset
			found = true
		}
		offset += cellBytes
	}
	if !found {
		insertOffset = offset
	}
	if insertOffset+reqBytes > PageSize {
		return ErrPageFull
	}
	if insertOffset < offset {
		copy(node.Data[insertOffset+reqBytes:offset+reqBytes], node.Data[insertOffset:offset])
	}
	binary.LittleEndian.PutUint16(node.Data[insertOffset:insertOffset+2], uint16(len(newKey)))
	binary.LittleEndian.PutUint16(node.Data[insertOffset+2:insertOffset+4], uint16(len(newVal)))
	keyStart := insertOffset + 4
	valStart := keyStart + uint32(len(newKey))
	copy(node.Data[keyStart:valStart], newKey)
	copy(node.Data[valStart:valStart+uint32(len(newVal))], newVal)
	node.SetNumCells(numCells + 1)
	node.MemVersion++
	return nil
}

func (tree *BTree) deleteLeafCell(node *BTreePage, offset, cellBytes uint32) error {
	var tailOffset uint32 = BTreeHeaderSize
	numCells := node.NumCells()
	for i := uint16(0); i < numCells; i++ {
		if tailOffset+4 > PageSize {
			break
		}
		kLen := uint32(binary.LittleEndian.Uint16(node.Data[tailOffset : tailOffset+2]))
		vLen := uint32(binary.LittleEndian.Uint16(node.Data[tailOffset+2 : tailOffset+4]))
		tailOffset += 4 + kLen + vLen
	}
	if offset+cellBytes > tailOffset || tailOffset > PageSize {
		return ErrPageFull
	}
	copy(node.Data[offset:tailOffset-cellBytes], node.Data[offset+cellBytes:tailOffset])
	for idx := tailOffset - cellBytes; idx < tailOffset; idx++ {
		node.Data[idx] = 0
	}
	node.SetNumCells(numCells - 1)
	node.MemVersion++
	return nil
}

func (tree *BTree) insertIntoInternal(node *BTreePage, pivotKey []byte, leftChildID, rightChildID PageID) error {
	reqBytes := uint32(10 + len(pivotKey))
	if !node.IsSafeForInsert(reqBytes) {
		return ErrPageFull
	}
	numCells := node.NumCells()
	var offset uint32 = BTreeHeaderSize
	insertOffset := uint32(0)
	found := false
	
	for i := uint16(0); i < numCells; i++ {
		if offset+10 > PageSize {
			break
		}
		kLen := uint32(binary.LittleEndian.Uint16(node.Data[offset : offset+2]))
		if offset+10+kLen > PageSize {
			break
		}
		
		cellKey := node.Data[offset+10 : offset+10+kLen]
		if !found && bytes.Compare(pivotKey, cellKey) < 0 {
			insertOffset = offset
			found = true
		}
		offset += 10 + kLen
	}
	if !found {
		insertOffset = offset
		node.SetRightmostChildID(rightChildID)
	} else {
		if offset+reqBytes <= PageSize {
			copy(node.Data[insertOffset+reqBytes:offset+reqBytes], node.Data[insertOffset:offset])
			binary.LittleEndian.PutUint64(node.Data[insertOffset+reqBytes+2:insertOffset+reqBytes+10], uint64(rightChildID))
		}
	}
	if insertOffset+reqBytes <= PageSize {
		binary.LittleEndian.PutUint16(node.Data[insertOffset:insertOffset+2], uint16(len(pivotKey)))
		binary.LittleEndian.PutUint64(node.Data[insertOffset+2:insertOffset+10], uint64(leftChildID))
		copy(node.Data[insertOffset+10:insertOffset+10+uint32(len(pivotKey))], pivotKey)
		node.SetNumCells(numCells + 1)
		node.MemVersion++
	}
	return nil
}
