package ultimate_db

import (
	"bytes"
	"testing"
)

func openTestDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := dir + "/kv.db"
	walPath := dbPath + "_wal.log"
	device, err := NewOSFileDevice(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	bp := NewBufferPool(NewDiskManager(device), 64, NewLRUEvictionPolicy(), NewAtomicMetrics())
	wal, err := NewBatchingWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}
	db := NewDB(bp, wal, NewAtomicMetrics())
	return db, dbPath
}

func TestUserPutGetSurvivesReopen(t *testing.T) {
	db, dbPath := openTestDB(t)
	defer func() { _ = db.Close() }()

	store := NewBTreeKVStoreWithRoot(db, BTreeRootPageID)
	key := []byte("data:user:restarttest")
	val := []byte(`{"id":"cmVzdGFydHRlc3Q","name":"restarttest","displayName":"restarttest","credentials":[]}`)
	mailKey := []byte("mail:box:alice@0trust.mesh:welcome")
	mailVal := []byte(`{"id":"welcome","owner":"alice@0trust.mesh","folder":"inbox","subject":"hi"}`)
	txn := store.Begin()
	if err := store.Put(txn, key, val, 0); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := store.Put(txn, mailKey, mailVal, 0); err != nil {
		t.Fatalf("mail put failed: %v", err)
	}
	txn.Commit()
	_ = store.Flush()

	txn2 := store.Begin()
	got, err := store.Get(txn2, key)
	if err != nil {
		t.Fatalf("get failed before reopen: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("unexpected value before reopen: %s", got)
	}

	if err := db.Close(); err != nil {
		t.Logf("close: %v", err)
	}

	device, err := NewOSFileDevice(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	bp := NewBufferPool(NewDiskManager(device), 64, NewLRUEvictionPolicy(), NewAtomicMetrics())
	wal, err := NewBatchingWAL(dbPath + "_wal.log")
	if err != nil {
		t.Fatal(err)
	}
	db2 := NewDB(bp, wal, NewAtomicMetrics())
	defer db2.Close()

	store2 := NewBTreeKVStoreWithRoot(db2, BTreeRootPageID)
	txn3 := store2.Begin()
	got2, err := store2.Get(txn3, key)
	if err != nil {
		t.Fatalf("get failed after reopen: %v", err)
	}
	if !bytes.Equal(got2, val) {
		t.Fatalf("unexpected value after reopen: %s", got2)
	}
	gotMail, err := store2.Get(txn3, mailKey)
	if err != nil {
		t.Fatalf("mail get failed after reopen: %v", err)
	}
	if !bytes.Equal(gotMail, mailVal) {
		t.Fatalf("mail value lost after reopen: %s", gotMail)
	}
}