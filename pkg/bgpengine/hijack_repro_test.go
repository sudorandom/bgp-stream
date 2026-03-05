package bgpengine

import (
	"encoding/binary"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func TestClassifier_HijackReproduction(t *testing.T) {
	// 1. Setup ASN Mapping
	asnMapping := utils.NewASNMapping()

	// 2. Setup seenDB
	seenDBPath := filepath.Join(t.TempDir(), "test-seen-repro.db")
	seenDB, err := utils.OpenDiskTrie(seenDBPath)
	if err != nil {
		t.Fatalf("OpenDiskTrie failed: %v", err)
	}
	defer func() { _ = seenDB.Close() }()

	// Add historical origin AS24203 for prefix 140.213.1.0/24
	oldASN := uint32(24203)
	asnData := make([]byte, 4)
	binary.BigEndian.PutUint32(asnData, oldASN)
	_, ipNet, _ := net.ParseCIDR("140.213.1.0/24")
	_ = seenDB.Insert(ipNet, asnData)

	c := NewClassifier(seenDB, nil, asnMapping, nil, func(p string) uint32 { return 0 }, nil, time.Now)
	now := time.Now()

	t.Run("Suppressed by Matching OrgID", func(t *testing.T) {
		utils.SetASNOrgID(asnMapping, 139994, "XLSMART-ORG")
		utils.SetASNOrgID(asnMapping, 24203, "XLSMART-ORG")

		s := &prefixStats{
			uniquePeers: map[string]bool{"p1": true, "p2": true, "p3": true},
			uniqueHosts: map[string]bool{"h1": true, "h2": true},
		}

		ctx := &MessageContext{
			OriginASN:      139994,
			LastRpkiStatus: int32(utils.RPKIInvalidASN),
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("140.213.1.0/24", s, 65.0, ctx)
		if ok && et == ClassificationRouteLeak {
			t.Errorf("Expected suppression (ClassificationNone), got %v", et)
		}
	})

	t.Run("Suppressed by Fuzzy Name Match (Fix Verification)", func(t *testing.T) {
		// Simulate missing OrgID but matching name prefixes
		utils.SetASNOrgID(asnMapping, 139994, "")
		utils.SetASNOrgID(asnMapping, 24203, "")
		utils.SetASNName(asnMapping, 139994, "XLSmart AS139994")
		utils.SetASNName(asnMapping, 24203, "XLSmart AS24203")

		s := &prefixStats{
			uniquePeers: map[string]bool{"p1": true, "p2": true, "p3": true},
			uniqueHosts: map[string]bool{"h1": true, "h2": true},
		}

		ctx := &MessageContext{
			OriginASN:      139994,
			LastRpkiStatus: int32(utils.RPKIInvalidASN),
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("140.213.1.0/24", s, 65.0, ctx)
		// This should return ClassificationNone (not ok) because names match on first word
		if ok && et == ClassificationRouteLeak {
			t.Errorf("Expected fuzzy name suppression, got %v", et)
		}
	})
}
