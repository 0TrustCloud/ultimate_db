package ultimate_db

import (
	"hash/crc32"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// ============================================================================
// 1. Production Slotted Page Architecture Layout
// ============================================================================

// Page represents a production-hardened 32KB slotted data block frame.
//
// The 32-byte header layout structure on disk is mapped as follows:
// [0:4]   CRC32 Checksum (Validates whole-page byte integrity)
// [4:8]   Page Type Flag (Leaf vs. Internal Node layout)
// [8:12]  Slot Count (Number of tracking elements in forwarding array)
// [12:16] Lower Free Space Boundary (Offset where slots directory ends; grows forward)
// [16:20] Upper Free Space Boundary (Offset where record payloads begin; grows backward)
// [24:32] Next Leaf Page ID (8-byte sequence pointer for B+ Tree leaf chain indexing)
type Page struct {
	ID         PageID
	Data       [PageSize]byte
	PinCount   atomic.Int32
	IsDirty    bool
	Latch      sync.RWMutex
	MemVersion uint64 // Used for Optimistic Concurrency Control (OCC) validation
}

// Init configures an empty, completely zeroed slotted page layout.
func (p *Page) Init() {
	p.SetChecksum(0)
	p.SetPageType(0)
	p.SetSlotCount(0)
	p.SetLowerBoundary(PageHeaderSize)
	p.SetUpperBoundary(PageSize)
	p.SetNextPageID(0)
	
	// Explicitly clear the remaining memory space matrix
	for i := PageHeaderSize; i < PageSize; i++ {
		p.Data[i] = 0
	}
}

// --- Header Accessors ---

func (p *Page) GetChecksum() uint32 {
	return binary.LittleEndian.Uint32(p.Data[0:4])
}

func (p *Page) SetChecksum(val uint32) {
	binary.LittleEndian.PutUint32(p.Data[0:4], val)
}

func (p *Page) GetPageType() uint32 {
	return binary.LittleEndian.Uint32(p.Data[4:8])
}

func (p *Page) SetPageType(t uint32) {
	binary.LittleEndian.PutUint32(p.Data[4:8], t)
}

func (p *Page) GetSlotCount() uint32 {
	return binary.LittleEndian.Uint32(p.Data[8:12])
}

func (p *Page) SetSlotCount(count uint32) {
	binary.LittleEndian.PutUint32(p.Data[8:12], count)
}

func (p *Page) GetLowerBoundary() uint32 {
	return binary.LittleEndian.Uint32(p.Data[12:16])
}

func (p *Page) SetLowerBoundary(offset uint32) {
	binary.LittleEndian.PutUint32(p.Data[12:16], offset)
}

func (p *Page) GetUpperBoundary() uint32 {
	return binary.LittleEndian.Uint32(p.Data[16:20])
}

func (p *Page) SetUpperBoundary(offset uint32) {
	binary.LittleEndian.PutUint32(p.Data[16:20], offset)
}

func (p *Page) GetNextPageID() PageID {
	return PageID(binary.LittleEndian.Uint64(p.Data[24:32]))
}

func (p *Page) SetNextPageID(id PageID) {
	binary.LittleEndian.PutUint64(p.Data[24:32], uint64(id))
}

// --- Slotted Space Allocation Primitives ---

// ComputeChecksum calculates the IEEE CRC32 across payload data bytes (skipping index 0:4).
func (p *Page) ComputeChecksum() uint32 {
	return crc32.ChecksumIEEE(p.Data[4:])
}

// Slot represents a directory entry mapping inside the forwarding array.
// Each slot consumes exactly 4 bytes: [0:2] Payload Offset, [2:4] Payload Length.
type Slot struct {
	Offset uint16
	Length uint16
}

func (p *Page) GetSlot(idx uint32) (Slot, error) {
	if idx >= p.GetSlotCount() {
		return Slot{}, errors.New("slot index out of range")
	}
	slotOffset := PageHeaderSize + (idx * 4)
	off := binary.LittleEndian.Uint16(p.Data[slotOffset : slotOffset+2])
	length := binary.LittleEndian.Uint16(p.Data[slotOffset+2 : slotOffset+4])
	return Slot{Offset: off, Length: length}, nil
}

func (p *Page) WriteSlot(idx uint32, s Slot) {
	slotOffset := PageHeaderSize + (idx * 4)
	binary.LittleEndian.PutUint16(p.Data[slotOffset:slotOffset+2], s.Offset)
	binary.LittleEndian.PutUint16(p.Data[slotOffset+2:slotOffset+4], s.Length)
}

// IsSafeForInsert verifies if fragmented free space remains to comfortably host structural expansion.
func (p *Page) IsSafeForInsert(requiredBytes uint32) bool {
	lower := p.GetLowerBoundary()
	upper := p.GetUpperBoundary()
	// Slot array directory grows forward (+4 bytes), tuple values move backward (-requiredBytes)
	return (lower + 4 + requiredBytes) <= upper
}

// ============================================================================
// 2. Abstract Disk Manager
// ============================================================================

type DiskManager struct {
	device BlockDevice // Decoupled storage medium (OSFileDevice, MemDevice)
}

func NewDiskManager(device BlockDevice) *DiskManager {
	return &DiskManager{device: device}
}

func (d *DiskManager) ReadPage(id PageID, data *[PageSize]byte) error {
	_, err := d.device.ReadAt(data[:], int64(id)*PageSize)
	return err
}

func (d *DiskManager) WritePage(id PageID, data *[PageSize]byte) error {
	_, err := d.device.WriteAt(data[:], int64(id)*PageSize)
	return err
}

// ============================================================================
// 3. Hardened Buffer Pool Management Layer
// ============================================================================

type BufferPool struct {
	disk      *DiskManager
	frames    []*Page
	pageTable map[PageID]int // Maps active PageID to explicit frame indexes
	evictor   EvictionPolicy // Pluggable cache replacement architecture
	freeList  []int
	metrics   EngineMetrics // Observability visibility tracker hook
	mu        sync.Mutex
}

func NewBufferPool(disk *DiskManager, poolSize int, evictor EvictionPolicy, metrics EngineMetrics) *BufferPool {
	bp := &BufferPool{
		disk:      disk,
		frames:    make([]*Page, poolSize),
		pageTable: make(map[PageID]int),
		evictor:   evictor,
		freeList:  make([]int, poolSize),
		metrics:   metrics,
	}
	for i := 0; i < poolSize; i++ {
		bp.frames[i] = &Page{}
		bp.freeList[i] = i
	}
	return bp
}

func (bp *BufferPool) FetchPage(id PageID) (*Page, error) {
	bp.mu.Lock()

	// Cache Hit
	if frameIdx, exists := bp.pageTable[id]; exists {
		if bp.metrics != nil {
			bp.metrics.IncrBufferPoolHit()
		}
		bp.evictor.RecordAccess(id)
		page := bp.frames[frameIdx]
		page.PinCount.Add(1)
		bp.mu.Unlock()
		return page, nil
	}

	// Cache Miss
	if bp.metrics != nil {
		bp.metrics.IncrBufferPoolMiss()
	}

	frameIdx, err := bp.getAvailableFrame()
	if err != nil {
		bp.mu.Unlock()
		return nil, err
	}

	page := bp.frames[frameIdx]
	page.ID = id
	page.PinCount.Store(1)
	page.IsDirty = false
	for i := range page.Data {
		page.Data[i] = 0
	}

	if err := bp.disk.ReadPage(id, &page.Data); err != nil && !errors.Is(err, io.EOF) {
		bp.freeList = append(bp.freeList, frameIdx)
		bp.mu.Unlock()
		return nil, err
	}

	// Check if the page is entirely blank (all zeroes). If it is, skip validation.
	isAllZero := true
	for i := 0; i < len(page.Data); i++ {
		if page.Data[i] != 0 {
			isAllZero = false
			break
		}
	}

	// B-tree KV pages (1000+) store type/cells in the header — skip slotted CRC validation.
	if !isAllZero && !isBTreeKVPage(id) {
		storedCRC := page.GetChecksum()
		computedCRC := page.ComputeChecksum()
		if storedCRC != computedCRC && storedCRC != 0 { // Guard against intermediate un-flushed writes
			bp.freeList = append(bp.freeList, frameIdx)
			bp.mu.Unlock()
			return nil, fmt.Errorf("CRITICAL BIT ROT CORRUPTION: Checksum failure detected on Page %d (Stored: 0x%X, Computed: 0x%X)", id, storedCRC, computedCRC)
		}
	}

	bp.pageTable[id] = frameIdx
	bp.evictor.RecordAccess(id)
	bp.mu.Unlock()
	return page, nil
}
func (bp *BufferPool) UnpinPage(id PageID, isDirty bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if frameIdx, exists := bp.pageTable[id]; exists {
		page := bp.frames[frameIdx]
		page.PinCount.Add(-1)
		if isDirty {
			page.IsDirty = true
		}
	}
}

func (bp *BufferPool) getAvailableFrame() (int, error) {
	// Production Backpressure Strategy: Identify dirty page saturation levels
	dirtyCount := 0
	for _, frameIdx := range bp.pageTable {
		if bp.frames[frameIdx].IsDirty {
			dirtyCount++
		}
	}
	
	// If over 80% of total cache cache lines are dirty, freeze loops and force sync cycles
	if len(bp.frames) > 0 && float64(dirtyCount)/float64(len(bp.frames)) >= 0.8 {
		bp.mu.Unlock()
		bp.FlushAll()
		bp.mu.Lock()
	}

	if len(bp.freeList) > 0 {
		idx := bp.freeList[0]
		bp.freeList = bp.freeList[1:]
		return idx, nil
	}

	for {
		victimID, ok := bp.evictor.Evict()
		if !ok {
			break
		}

		frameIdx, exists := bp.pageTable[victimID]
		if !exists {
			continue
		}

		victimPage := bp.frames[frameIdx]
		if victimPage.PinCount.Load() > 0 {
			bp.evictor.RecordAccess(victimID) // Re-record historical priority to break deadlock
			continue
		}

		if victimPage.IsDirty {
			if !isBTreeKVPage(victimPage.ID) {
				victimPage.SetChecksum(victimPage.ComputeChecksum())
			}
			if err := bp.disk.WritePage(victimPage.ID, &victimPage.Data); err != nil {
				bp.evictor.RecordAccess(victimID)
				return -1, err
			}
		}

		delete(bp.pageTable, victimID)
		return frameIdx, nil
	}

	return -1, errors.New("buffer pool completely exhausted: all tracking frames are pinned")
}

func (bp *BufferPool) NewPage() (*Page, error) {
	bp.mu.Lock()
	stat, err := bp.disk.device.Stat()
	if err != nil {
		bp.mu.Unlock()
		return nil, err
	}
	
	newID := PageID(stat.Size() / PageSize)
	empty := [PageSize]byte{}
	if err := bp.disk.WritePage(newID, &empty); err != nil {
		bp.mu.Unlock()
		return nil, err
	}
	bp.mu.Unlock()
	
	p, err := bp.FetchPage(newID)
	if err == nil {
		p.Init()
	}
	return p, err
}

func isBTreeKVPage(id PageID) bool {
	return id >= BTreeRootPageID-2 // pages 1000+ use B-tree headers, not slotted CRC
}

func (bp *BufferPool) FlushAll() error {
	bp.mu.Lock()
	var dirtyPages []*Page

	for _, frameIdx := range bp.pageTable {
		page := bp.frames[frameIdx]
		if page.IsDirty {
			page.PinCount.Add(1)
			dirtyPages = append(dirtyPages, page)
		}
	}
	bp.mu.Unlock()

	var firstErr error
	for _, page := range dirtyPages {
		page.Latch.Lock()
		if !isBTreeKVPage(page.ID) {
			page.SetChecksum(page.ComputeChecksum())
		}
		err := bp.disk.WritePage(page.ID, &page.Data)
		
		page.Latch.Unlock()

		bp.mu.Lock()
		if err == nil {
			page.IsDirty = false
		} else if firstErr == nil {
			firstErr = err
		}
		page.PinCount.Add(-1)
		bp.mu.Unlock()
	}

	// CRITICAL PRODUCTION DURABILITY BARRIER: Invoke standard OS kernel fsync
	bp.mu.Lock()
	syncErr := bp.disk.device.Sync()
	if firstErr == nil {
		firstErr = syncErr
	}
	bp.mu.Unlock()

	return firstErr
}
