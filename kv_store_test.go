package ultimate_db

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestRecoverJSONKVFromRaw(t *testing.T) {
	raw := make([]byte, PageSize)
	payload := []byte(`data:user:admin{"id":"YWRtaW4=","name":"admin","displayName":"admin","credentials":[]}`)
	copy(raw[100:], payload)

	got := recoverJSONKVFromRaw(raw)
	val, ok := got["data:user:admin"]
	if !ok {
		t.Fatalf("expected data:user:admin, got %#v", got)
	}
	if !bytes.Contains(val, []byte(`"name":"admin"`)) {
		t.Fatalf("unexpected value: %s", val)
	}
}

func TestRecoverSlottedKVFromRaw(t *testing.T) {
	raw := make([]byte, PageSize)
	key := []byte("data:oidc_master_key")
	val := []byte{0x30, 0x82, 0x04, 0xa3}
	offset := 64
	binary.LittleEndian.PutUint64(raw[offset:], 1)
	binary.LittleEndian.PutUint64(raw[offset+8:], 0)
	binary.LittleEndian.PutUint32(raw[offset+16:], uint32(len(key)))
	binary.LittleEndian.PutUint32(raw[offset+20:], uint32(len(val)))
	copy(raw[offset+24:], key)
	copy(raw[offset+24+len(key):], val)

	got := recoverSlottedKVFromRaw(raw)
	if !bytes.Equal(got["data:oidc_master_key"], val) {
		t.Fatalf("unexpected recovered signing key: %#v", got["data:oidc_master_key"])
	}
}