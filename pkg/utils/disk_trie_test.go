package utils

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestDiskTrie(t *testing.T) {
	// Create a temporary directory for the test database
	tmpDir, err := os.MkdirTemp("", "disktrie-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// 1. Test Open and Close
	trie, err := OpenDiskTrie(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DiskTrie: %v", err)
	}

	// 2. Test Basic Insert and Lookup
	_, ipNet, _ := net.ParseCIDR("1.2.3.0/24")
	val := []byte("test-value")
	if err := trie.Insert(ipNet, val); err != nil {
		t.Errorf("Insert failed: %v", err)
	}

	res, mask, err := trie.Lookup(net.ParseIP("1.2.3.4"))
	if err != nil {
		t.Errorf("Lookup failed: %v", err)
	}
	if !bytes.Equal(res, val) || mask != 24 {
		t.Errorf("Lookup mismatch: got (%s, %d), want (%s, 24)", res, mask, val)
	}

	// 3. Test Longest Prefix Match (LPM)
	_, ipNetSpecific, _ := net.ParseCIDR("1.2.3.128/25")
	valSpecific := []byte("specific-value")
	if err := trie.Insert(ipNetSpecific, valSpecific); err != nil {
		t.Errorf("Insert specific failed: %v", err)
	}

	// Should match the more specific /25
	res, mask, err = trie.Lookup(net.ParseIP("1.2.3.129"))
	if err != nil {
		t.Errorf("LPM Lookup failed: %v", err)
	}
	if !bytes.Equal(res, valSpecific) || mask != 25 {
		t.Errorf("LPM mismatch: got (%s, %d), want (%s, 25)", res, mask, valSpecific)
	}

	// Should still match the /24 for other IPs in that range
	res, mask, err = trie.Lookup(net.ParseIP("1.2.3.1"))
	if err != nil {
		t.Errorf("General Lookup failed: %v", err)
	}
	if !bytes.Equal(res, val) || mask != 24 {
		t.Errorf("General mismatch: got (%s, %d), want (%s, 24)", res, mask, val)
	}

	// 4. Test Batch Insert
	batch := map[string][]byte{
		"10.0.0.0/8":  []byte("private-a"),
		"10.1.0.0/16": []byte("private-a-sub"),
	}
	if err := trie.BatchInsert(batch); err != nil {
		t.Errorf("BatchInsert failed: %v", err)
	}

	res, mask, err = trie.Lookup(net.ParseIP("10.1.2.3"))
	if err != nil {
		t.Errorf("Batch Lookup failed: %v", err)
	}
	if !bytes.Equal(res, batch["10.1.0.0/16"]) || mask != 16 {
		t.Errorf("Batch mismatch: got (%s, %d), want (private-a-sub, 16)", res, mask)
	}

	// 5. Test Persistence (Close and Reopen)
	if err := trie.Close(); err != nil {
		t.Fatalf("Failed to close trie: %v", err)
	}

	trie, err = OpenDiskTrie(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen DiskTrie: %v", err)
	}
	defer trie.Close()

	res, mask, err = trie.Lookup(net.ParseIP("1.2.3.129"))
	if err != nil {
		t.Errorf("Lookup after reopen failed: %v", err)
	}
	if !bytes.Equal(res, valSpecific) || mask != 25 {
		t.Errorf("Persistence mismatch: got (%s, %d), want (%s, 25)", res, mask, valSpecific)
	}

	// 6. Test Non-existent Lookup
	res, mask, err = trie.Lookup(net.ParseIP("8.8.8.8"))
	if err != nil {
		t.Errorf("Non-existent lookup errored: %v", err)
	}
	if res != nil {
		t.Errorf("Expected nil for non-existent IP, got %s", res)
	}

	// 7. Test BatchInsertRaw and Get
	rawBatch := map[string][]byte{
		"raw-key-1": []byte("raw-val-1"),
		"raw-key-2": []byte("raw-val-2"),
	}
	if err := trie.BatchInsertRaw(rawBatch); err != nil {
		t.Errorf("BatchInsertRaw failed: %v", err)
	}

	for k, v := range rawBatch {
		res, err := trie.Get(k)
		if err != nil {
			t.Errorf("Get failed for %s: %v", k, err)
		}
		if !bytes.Equal(res, v) {
			t.Errorf("Get mismatch for %s: got %s, want %s", k, res, v)
		}
	}
}

func TestDiskTrieComplexSubnets(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "disktrie-complex-*")
	defer os.RemoveAll(tmpDir)
	trie, _ := OpenDiskTrie(filepath.Join(tmpDir, "test.db"))
	defer trie.Close()

	// Insert a variety of overlapping and adjacent subnets
	subnets := map[string]string{
		"0.0.0.0/0":      "default",
		"10.0.0.0/8":     "private-a",
		"10.1.0.0/16":    "private-a-sub",
		"10.1.1.0/24":    "private-a-sub-sub",
		"10.1.1.1/32":    "specific-host",
		"192.168.1.0/24": "private-c",
		"192.168.1.1/32": "private-c-host",
		"172.16.0.0/12":  "private-b",
	}

	for cidr, label := range subnets {
		_, ipNet, _ := net.ParseCIDR(cidr)
		if err := trie.Insert(ipNet, []byte(label)); err != nil {
			t.Fatalf("Failed to insert %s: %v", cidr, err)
		}
	}

	tests := []struct {
		ip       string
		want     string
		wantMask int
	}{
		{"10.1.1.1", "specific-host", 32},
		{"10.1.1.2", "private-a-sub-sub", 24},
		{"10.1.2.1", "private-a-sub", 16},
		{"10.2.1.1", "private-a", 8},
		{"192.168.1.1", "private-c-host", 32},
		{"192.168.1.2", "private-c", 24},
		{"172.16.0.1", "private-b", 12},
		{"172.31.255.255", "private-b", 12},
		{"172.32.0.1", "default", 0},
		{"8.8.8.8", "default", 0},
		{"1.1.1.1", "default", 0},
	}

	for _, tt := range tests {
		t.Run(tt.ip, func(t *testing.T) {
			res, mask, err := trie.Lookup(net.ParseIP(tt.ip))
			if err != nil {
				t.Errorf("Lookup failed for %s: %v", tt.ip, err)
			}
			if string(res) != tt.want || mask != tt.wantMask {
				t.Errorf("Lookup(%s) = (%s, %d), want (%s, %d)", tt.ip, res, mask, tt.want, tt.wantMask)
			}
		})
	}
}

func TestDiskTrieIPv6Error(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "disktrie-v6-*")
	defer os.RemoveAll(tmpDir)
	trie, _ := OpenDiskTrie(filepath.Join(tmpDir, "test.db"))
	defer trie.Close()

	// Currently only IPv4 is supported
	ip := net.ParseIP("2001:db8::1")
	_, _, err := trie.Lookup(ip)
	if err == nil {
		t.Error("Expected error for IPv6 lookup, got nil")
	}

	_, ipNet, _ := net.ParseCIDR("2001:db8::/32")
	err = trie.Insert(ipNet, []byte("fail"))
	if err == nil {
		t.Error("Expected error for IPv6 insert, got nil")
	}
}
