package ultimate_db

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func openReplicaTestDB(t *testing.T, dir string) (*DB, string) {
	t.Helper()
	dbPath := filepath.Join(dir, "platform.db")
	device, err := NewOSFileDevice(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	bp := NewBufferPool(NewDiskManager(device), 64, NewLRUEvictionPolicy(), NewAtomicMetrics())
	wal, err := NewBatchingWAL(dbPath + "_wal.log")
	if err != nil {
		t.Fatal(err)
	}
	db := NewDB(bp, wal, NewAtomicMetrics())
	return db, dbPath
}

func TestIdentityReplicaSurvivesPrimaryWipe(t *testing.T) {
	dir := t.TempDir()
	db, dbPath := openReplicaTestDB(t, dir)
	defer func() { _ = db.Close() }()

	store := NewBTreeKVStore(db)
	key := []byte("data:user:admin")
	val := []byte(`{"id":"YWRtaW4=","name":"admin","displayName":"admin","credentials":[{"id":"yubikey1"}]}`)

	txn := store.Begin()
	if err := store.Put(txn, key, val, 0); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Mirror file must exist after durable put.
	mirror := dbPath + ".identity.json"
	if _, err := os.Stat(mirror); err != nil {
		t.Fatalf("expected identity mirror at %s: %v", mirror, err)
	}

	// Simulate primary-tree loss (the historical "update wiped passkeys" failure).
	rep, ok := store.(*ReplicatingKVStore)
	if !ok {
		t.Fatalf("expected *ReplicatingKVStore, got %T", store)
	}
	wTxn := rep.primary.Begin()
	_ = rep.primary.Delete(wTxn, key)
	_ = wTxn.Commit()
	_ = rep.primary.Flush()

	// Get must rehydrate from replica/mirror.
	rTxn := store.Begin()
	got, err := store.Get(rTxn, key)
	_ = rTxn.Commit()
	if err != nil {
		t.Fatalf("get after primary wipe: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("credentials not restored: %s", got)
	}

	if err := db.Close(); err != nil {
		t.Logf("close: %v", err)
	}

	// Reopen fresh process — keys must still be present.
	db2, _ := openReplicaTestDB(t, dir)
	defer func() { _ = db2.Close() }()
	store2 := NewBTreeKVStore(db2)
	txn2 := store2.Begin()
	got2, err := store2.Get(txn2, key)
	_ = txn2.Commit()
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if !bytes.Equal(got2, val) {
		t.Fatalf("credentials lost across reopen: %s", got2)
	}
}

func TestIdentityReplicaRehydrateFromMirrorOnly(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "platform.db")
	mirror := dbPath + ".identity.json"

	doc := identityMirrorFile{
		Version: 1,
		Keys: map[string][]byte{
			"data:user:alice": []byte(`{"name":"alice","credentials":[{"id":"k1"}]}`),
		},
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mirror, b, 0o600); err != nil {
		t.Fatal(err)
	}

	db, _ := openReplicaTestDB(t, dir)
	defer db.Close()
	store := NewBTreeKVStore(db)

	txn := store.Begin()
	got, err := store.Get(txn, []byte("data:user:alice"))
	_ = txn.Commit()
	if err != nil {
		t.Fatalf("rehydrate from mirror: %v", err)
	}
	if !bytes.Contains(got, []byte(`"name":"alice"`)) {
		t.Fatalf("unexpected user blob: %s", got)
	}
}
