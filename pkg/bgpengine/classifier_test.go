package bgpengine

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"path/filepath"

	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func TestClassifier_HasRouteLeak(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil, time.Now)

	tests := []struct {
		name    string
		pathStr string
		wantOk  bool
	}{
		{
			name:    "Empty Path",
			pathStr: "",
			wantOk:  false,
		},
		{
			name:    "Short Path",
			pathStr: "[100 200]",
			wantOk:  false,
		},
		{
			name:    "Valid Tier-1 Transit",
			pathStr: "[174 1239 100]",
			wantOk:  false,
		},
		{
			name:    "Route Leak (Tier-1 -> Non-Tier-1 -> Tier-1)",
			pathStr: "[174 100 1239]",
			wantOk:  true,
		},
		{
			name:    "No Leak (Tier-1 -> Cloud -> Tier-1)",
			pathStr: "[174 15169 1239]",
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &MessageContext{PathStr: tt.pathStr}
			_, ok := c.hasRouteLeak(ctx)
			if ok != tt.wantOk {
				t.Errorf("hasRouteLeak() ok = %v, want %v", ok, tt.wantOk)
			}
		})
	}
}

func TestClassifier_FindCriticalAnomaly(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil, time.Now)
	now := time.Now()

	t.Run("Outage Detection", func(t *testing.T) {
		s := &prefixStats{totalWith: 30, totalAnn: 0}
		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, &MessageContext{Now: now})
		if !ok || et != ClassificationOutage {
			t.Errorf("findCriticalAnomaly() expected Outage, got %v, %v", et, ok)
		}
	})

	t.Run("Hijack High Signal Detection", func(t *testing.T) {
		// Mock seen DB for historical origin
		seenDBPath := filepath.Join(t.TempDir(), "test-seen-classifier.db")
		seenDB, _ := utils.OpenDiskTrie(seenDBPath)
		defer func() { _ = seenDB.Close() }()

		oldASN := uint32(100)
		asnData := make([]byte, 4)
		binary.BigEndian.PutUint32(asnData, oldASN)
		_, ipNet, _ := net.ParseCIDR("1.1.1.0/24")
		err := seenDB.Insert(ipNet, asnData)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		c.seenDB = seenDB

		s := &prefixStats{
			uniquePeers: map[string]bool{"p1": true, "p2": true, "p3": true},
			uniqueHosts: map[string]bool{"h1": true, "h2": true},
		}

		ctx := &MessageContext{
			OriginASN:      200, // Different from 100
			LastRpkiStatus: int32(utils.RPKIInvalidASN),
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, ctx)
		if !ok || et != ClassificationRouteLeak {
			t.Errorf("findCriticalAnomaly() expected RouteLeak, got %v, %v", et, ok)
		}
	})
}
