package utils

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRPKIManager_Validate(t *testing.T) {
	dbPath := "test-rpki.db"
	_ = os.RemoveAll(dbPath)
	defer func() {
		_ = os.RemoveAll(dbPath)
	}()

	m, err := NewRPKIManager(dbPath)
	if err != nil {
		t.Fatalf("Failed to create RPKIManager: %v", err)
	}
	defer func() {
		_ = m.Close()
	}()

	// Setup mock VRPs
	vrpData := map[string][]VRP{
		"1.1.0.0/16": {
			{Prefix: "1.1.0.0/16", MaxLength: 24, ASN: 100},
		},
		"2.2.0.0/16": {
			{Prefix: "2.2.0.0/16", MaxLength: 24, ASN: 200},
		},
	}

	encodedMap := make(map[string][]byte)
	for prefix, v := range vrpData {
		b, _ := json.Marshal(v)
		encodedMap[prefix] = b
	}

	if err := m.trie.BatchInsert(encodedMap); err != nil {
		t.Fatalf("Failed to insert mock VRPs: %v", err)
	}

	tests := []struct {
		name      string
		prefix    string
		originASN uint32
		want      RPKIStatus
	}{
		{
			name:      "Valid Announcement",
			prefix:    "1.1.1.0/24",
			originASN: 100,
			want:      RPKIValid,
		},
		{
			name:      "Invalid ASN",
			prefix:    "1.1.1.0/24",
			originASN: 999,
			want:      RPKIInvalidASN,
		},
		{
			name:      "Invalid MaxLength",
			prefix:    "2.2.2.0/25",
			originASN: 200,
			want:      RPKIInvalidMaxLength,
		},
		{
			name:      "Valid Subnet within MaxLength",
			prefix:    "2.2.2.0/24",
			originASN: 200,
			want:      RPKIValid,
		},
		{
			name:      "Unknown Prefix",
			prefix:    "3.3.3.0/24",
			originASN: 300,
			want:      RPKIUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := m.Validate(tt.prefix, tt.originASN)
			if err != nil {
				t.Errorf("%s: Validate() error = %v", tt.name, err)
				return
			}
			if got != tt.want {
				t.Errorf("%s: Validate() got = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
