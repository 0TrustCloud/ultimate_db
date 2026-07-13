package ultimate_db

import (
	"os"
	"testing"
)

func TestHotSwapKVStoreReadThroughLegacy(t *testing.T) {
	dir := "data/test-hotswap"
	_ = os.RemoveAll(dir)
	_ = os.MkdirAll(dir, 0o755)
	db, err := openHotSwapTestDB(dir + "/test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	legacyPage := PageID(42)
	hotRoot := PageID(1014)
	marker := []byte("test:migrated")

	match := func(key []byte) bool {
		return string(key) == "story:one"
	}

	txn := db.BeginTxn()
	if err := db.Write(legacyPage, txn, []byte("story:one"), []byte("legacy-value"), 0); err != nil {
		t.Fatal(err)
	}
	db.CommitTxn(txn)

	store := NewHotSwapKVStore(HotSwapConfig{
		DB:          db,
		HotRoot:     hotRoot,
		LegacyPage:  legacyPage,
		MarkerKey:   marker,
		LegacyMatch: match,
		Label:       "test",
	})

	readTxn := store.Begin()
	val, err := store.Get(readTxn, []byte("story:one"))
	_ = readTxn.Commit()
	if err != nil || string(val) != "legacy-value" {
		t.Fatalf("legacy read-through = %q err=%v", val, err)
	}

	putTxn := store.Begin()
	if err := store.Put(putTxn, []byte("story:two"), []byte("hot-only"), 0); err != nil {
		t.Fatal(err)
	}
	if err := putTxn.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := store.MigrateLegacy(); err != nil {
		t.Fatal(err)
	}

	readTxn = store.Begin()
	val, err = store.Get(readTxn, []byte("story:one"))
	_ = readTxn.Commit()
	if err != nil || string(val) != "legacy-value" {
		t.Fatalf("post-migrate read = %q err=%v", val, err)
	}
}

func openHotSwapTestDB(path string) (*DB, error) {
	device, err := NewOSFileDevice(path)
	if err != nil {
		return nil, err
	}
	dm := NewDiskManager(device)
	evictor := NewLRUEvictionPolicy()
	metrics := NewAtomicMetrics()
	bp := NewBufferPool(dm, 1024, evictor, metrics)
	wal, err := NewBatchingWAL(path + ".wal")
	if err != nil {
		return nil, err
	}
	db := NewDB(bp, wal, metrics)
	if err := PerformRecovery(db, path+".wal"); err != nil {
		return nil, err
	}
	return db, nil
}