package ultimate_db

import "testing"

func TestResetBTreePage(t *testing.T) {
	page := &Page{ID: BTreeRootPageID}
	for i := range page.Data {
		page.Data[i] = 0xFF
	}
	node := &BTreePage{page}
	resetBTreePage(node)
	if node.PageType() != PageTypeLeaf {
		t.Fatalf("type=%d want leaf", node.PageType())
	}
	if node.NumCells() != 0 {
		t.Fatalf("cells=%d want 0", node.NumCells())
	}
}