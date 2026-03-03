package bgpengine

import (
	"encoding/binary"
	"testing"
	"time"

	"path/filepath"

	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func TestClassifier_HasRouteLeak(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil)

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
	c := NewClassifier(nil, nil, nil, nil, nil, nil)
	now := time.Now()

	t.Run("Outage Detection", func(t *testing.T) {
		s := &prefixStats{totalWith: 3, totalAnn: 0}
		et, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, &MessageContext{Now: now})
		if !ok || et != ClassificationOutage {
			t.Errorf("findCriticalAnomaly() expected Outage, got %v, %v", et, ok)
		}
	})

	t.Run("Hijack High Signal Detection", func(t *testing.T) {
		// Mock seen DB for historical origin
		seenDBPath := filepath.Join(t.TempDir(), "test-seen-classifier.db")
		seenDB, _ := utils.OpenDiskTrie(seenDBPath)
		defer func() { _ = seenDB.Close() }()

		asnBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(asnBytes, 100)
		_ = seenDB.BatchInsertRaw(map[string][]byte{"1.1.1.0/24": asnBytes})

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

		et, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, ctx)
		if !ok || et != ClassificationRouteLeak {
			t.Errorf("findCriticalAnomaly() expected RouteLeak, got %v, %v", et, ok)
		}
	})
}

func TestClassifier_FindBadAnomaly(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil)

	tests := []struct {
		name        string
		stats       *prefixStats
		elapsed     float64
		perPeerRate float64
		wantEt      ClassificationType
		wantOk      bool
	}{
		{
			name:        "AggFlap Detection",
			stats:       &prefixStats{totalAgg: 10},
			elapsed:     100.0,
			perPeerRate: 1.0,
			wantEt:      ClassificationAggFlap,
			wantOk:      true,
		},
		{
			name:        "NextHopOscillation Detection",
			stats:       &prefixStats{uniqueHops: map[string]bool{"h1": true, "h2": true}, totalHop: 10, totalPath: 2},
			elapsed:     100.0,
			perPeerRate: 1.0,
			wantEt:      ClassificationNextHopOscillation,
			wantOk:      true,
		},
		{
			name:        "LinkFlap Detection",
			stats:       &prefixStats{totalWith: 5, totalAnn: 9},
			elapsed:     100.0,
			perPeerRate: 1.0,
			wantEt:      ClassificationLinkFlap,
			wantOk:      true,
		},
		{
			name:        "Babbling Detection",
			stats:       &prefixStats{totalMsgs: 20, totalPath: 0, totalComm: 0, totalMed: 0, totalLP: 0, totalAgg: 0, totalHop: 0},
			elapsed:     100.0,
			perPeerRate: 2.0,
			wantEt:      ClassificationBabbling,
			wantOk:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			et, ok := c.findBadAnomaly(tt.stats, tt.elapsed, tt.perPeerRate)
			if ok != tt.wantOk || et != tt.wantEt {
				t.Errorf("findBadAnomaly() et = %v, ok = %v, want %v, %v", et, ok, tt.wantEt, tt.wantOk)
			}
		})
	}
}

func TestClassifier_FindNormalAnomaly(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil)

	tests := []struct {
		name    string
		stats   *prefixStats
		elapsed float64
		wantEt  ClassificationType
		wantOk  bool
	}{
		{
			name:    "PathHunting Detection",
			stats:   &prefixStats{totalAnn: 5, totalIncreases: 2, totalWith: 1},
			elapsed: 100.0,
			wantEt:  ClassificationPathHunting,
			wantOk:  true,
		},
		{
			name:    "PolicyChurn Detection",
			stats:   &prefixStats{totalComm: 10},
			elapsed: 100.0,
			wantEt:  ClassificationPolicyChurn,
			wantOk:  true,
		},
		{
			name:    "PathLengthOscillation Detection",
			stats:   &prefixStats{totalIncreases: 3, totalDecreases: 2},
			elapsed: 100.0,
			wantEt:  ClassificationPathLengthOscillation,
			wantOk:  true,
		},
		{
			name:    "Discovery Detection",
			stats:   &prefixStats{totalMsgs: 100},
			elapsed: 100.0,
			wantEt:  ClassificationDiscovery,
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			et, ok := c.findNormalAnomaly(tt.stats, tt.elapsed)
			if ok != tt.wantOk || et != tt.wantEt {
				t.Errorf("findNormalAnomaly() et = %v, ok = %v, want %v, %v", et, ok, tt.wantEt, tt.wantOk)
			}
		})
	}
}

func TestClassifier_IsHelpers(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil)

	if !c.isTier1(174) {
		t.Errorf("isTier1(174) expected true")
	}
	if c.isTier1(123456) {
		t.Errorf("isTier1(123456) expected false")
	}

	if !c.isCloud(15169) {
		t.Errorf("isCloud(15169) expected true")
	}
	if c.isCloud(123456) {
		t.Errorf("isCloud(123456) expected false")
	}

	if !c.isDDoSProvider(13335) {
		t.Errorf("isDDoSProvider(13335) expected true")
	}
	if c.isDDoSProvider(123456) {
		t.Errorf("isDDoSProvider(123456) expected false")
	}
}
