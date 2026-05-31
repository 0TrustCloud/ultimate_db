package ultimate_db

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// -----------------------------------------------------------------------------
// 1. Concrete Structs implementing NetworkTransport and AuditInterceptor
// -----------------------------------------------------------------------------

type TestNetworkAdapter struct {
	NodeID string
}

func (m *TestNetworkAdapter) BroadcastQuery(ctx context.Context, queryPayload []byte) ([][]byte, error) {
	mockRemoteHit := []SearchResult{
		{DocID: "remote-doc-99", Score: 8.5},
	}
	respBytes, _ := json.Marshal(mockRemoteHit)
	return [][]byte{respBytes}, nil
}

func (m *TestNetworkAdapter) GetLocalNodeID() string {
	return m.NodeID
}

type TestSecurityAdapter struct {
	AllowAll bool
}

func (t *TestSecurityAdapter) VerifyAccess(subject []byte, action, resource string) bool {
	return t.AllowAll
}

func (t *TestSecurityAdapter) LogAudit(actor, action, msg string) {
	// Traced safely during tests
}

// -----------------------------------------------------------------------------
// 2. Test Suite Helpers
// -----------------------------------------------------------------------------

func setupTestIntegratedEngine(t *testing.T, dbFile, walFile string, allowAccess bool) (*IntegratedEngine, func()) {
	_ = os.Remove(dbFile)
	_ = os.Remove(walFile)

	device, err := NewOSFileDevice(dbFile)
	if err != nil {
		t.Fatalf("Failed to initialize transient OS file device: %v", err)
	}

	disk := NewDiskManager(device)
	evictor := NewLRUEvictionPolicy()
	metrics := NewAtomicMetrics()
	bp := NewBufferPool(disk, 50, evictor, metrics)

	wal, err := NewBatchingWAL(walFile)
	if err != nil {
		t.Fatalf("Failed to instantiate WAL log frame: %v", err)
	}

	db := NewDB(bp, wal, metrics)

	for i := 0; i <= 12; i++ {
		_, _ = bp.NewPage()
	}

	transport := &TestNetworkAdapter{NodeID: "local-node-test"}
	interceptor := &TestSecurityAdapter{AllowAll: allowAccess}

	engine, err := NewIntegratedEngine(db, transport, interceptor)
	if err != nil {
		t.Fatalf("Kernel integration bootstrap failed: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(dbFile)
		_ = os.Remove(walFile)
	}

	return engine, cleanup
}

// -----------------------------------------------------------------------------
// 3. Execution Test Paths
// -----------------------------------------------------------------------------

func TestEngine_KernelSinglePassIndexingAndSearch(t *testing.T) {
	engine, cleanup := setupTestIntegratedEngine(t, "kernel_unit.db", "kernel_unit.wal", true)
	defer cleanup()

	targetPage := PageID(12)
	// Fixed: Changed from IndexDocument to InsertDocument
	err := engine.InsertDocument(targetPage, "doc-a", "critical adversarial ai anomaly verified")
	if err != nil {
		t.Fatalf("Single-pass atomic ingestion phase failed: %v", err)
	}

	// Fixed: Changed from IndexDocument to InsertDocument
	err = engine.InsertDocument(targetPage, "doc-b", "hardware tpm authentication loop initiated")
	if err != nil {
		t.Fatalf("Single-pass atomic ingestion phase failed: %v", err)
	}

	localHits, err := engine.LocalSearch("adversarial ai", 10)
	if err != nil {
		t.Fatalf("Local full-text search pass threw unexpected execution error: %v", err)
	}

	if len(localHits) == 0 {
		t.Fatal("Search step returned an empty slice of hits; index postings missing or unreadable")
	}

	if localHits[0].DocID != "doc-a" {
		t.Errorf("Relevance processing error: expected highest match to be doc-a, got %s", localHits[0].DocID)
	}
}

func TestEngine_ScatterGatherSecurityInterception(t *testing.T) {
	engine, cleanup := setupTestIntegratedEngine(t, "sec_unit.db", "sec_unit.wal", false)
	defer cleanup()

	ctx := context.Background()
	_, err := engine.ScatterGather(ctx, []byte("malicious-actor-token"), "adversarial ai", 5)

	if err == nil {
		t.Fatal("Security vulnerability detected: IntegratedEngine executed scatter-gather query despite inline ABAC rejection")
	}

	if !strings.Contains(err.Error(), "security policy validation failure") {
		t.Errorf("Unexpected error messaging context returned: %v", err)
	}
}

func TestEngine_ScatterGatherDistributedMerging(t *testing.T) {
	engine, cleanup := setupTestIntegratedEngine(t, "merge_unit.db", "merge_unit.wal", true)
	defer cleanup()

	targetPage := PageID(12)
	// Fixed: Changed from IndexDocument to InsertDocument
	_ = engine.InsertDocument(targetPage, "doc-local", "critical threat detected")

	ctx := context.Background()
	results, err := engine.ScatterGather(ctx, []byte("authorized-admin"), "critical threat", 10)
	if err != nil {
		t.Fatalf("Distributed search processing error: %v", err)
	}

	if len(results) < 2 {
		t.Fatalf("Merge failure: expected combined cluster results list, got length %d", len(results))
	}

	for i := 0; i < len(results)-1; i++ {
		if results[i].Score < results[i+1].Score {
			t.Errorf("Cluster scatter-gather results failed sorting validation constraints at position %d", i)
		}
	}
}
