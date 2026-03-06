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

func TestClassifier_SiblingRouteLeak(t *testing.T) {
	m := utils.NewASNMapping()

	// Create scenario where endpoints are siblings but have different ASNs
	// Example: SPRINT (1239) and SPRINT-B (1240)
	utils.SetASNName(m, 174, "COGENT")
	utils.SetASNName(m, 100, "NON-TIER-1")
	utils.SetASNName(m, 1239, "SPRINT-A")
	utils.SetASNName(m, 1240, "SPRINT-B")

	utils.SetASNOrgID(m, 174, "ORG-COGENT")
	utils.SetASNOrgID(m, 100, "ORG-STUB")
	utils.SetASNOrgID(m, 1239, "ORG-SPRINT")
	utils.SetASNOrgID(m, 1240, "ORG-SPRINT") // Sibling via OrgID

	c := NewClassifier(nil, nil, m, nil, nil, nil, time.Now)

	// Test 1: Actual Leak between different Orgs
	ctx1 := &MessageContext{PathStr: "[174 100 1239]"}
	if _, ok := c.hasRouteLeak(ctx1); !ok {
		t.Errorf("Expected route leak for [174 100 1239] but got none")
	}

	// Test 2: Sibling endpoints should NOT be a leak (Traffic Engineering)
	ctx2 := &MessageContext{PathStr: "[1239 100 1240]"}
	if _, ok := c.hasRouteLeak(ctx2); ok {
		t.Errorf("Expected NO route leak for sibling endpoints [1239 100 1240], but one was detected")
	}

	// Test 3: Sibling via Name Fallback (Hyphen split test)
	utils.SetASNName(m, 2914, "NTT-AMERICA")
	utils.SetASNName(m, 2915, "NTT-EUROPE")
	utils.SetASNOrgID(m, 2914, "ORG-NTT-AM") // Different OrgIDs to test fallback
	utils.SetASNOrgID(m, 2915, "ORG-NTT-EU")

	ctx3 := &MessageContext{PathStr: "[2914 100 2915]"}
	if _, ok := c.hasRouteLeak(ctx3); ok {
		t.Errorf("Expected NO route leak for sibling endpoints (via Name Fallback) [2914 100 2915], but one was detected")
	}
}

func TestClassifier_FindCriticalAnomaly(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil, time.Now)
	now := time.Now()

	t.Run("Outage Detection", func(t *testing.T) {
		s := &prefixStats{
			totalWith:      30,
			totalAnn:       0,
			withdrawnPeers: map[string]bool{"p1": true, "p2": true, "p3": true, "p4": true, "p5": true, "p6": true, "p7": true, "p8": true, "p9": true, "p10": true},
			withdrawnHosts: map[string]bool{"h1": true, "h2": true, "h3": true},
		}
		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, &MessageContext{Now: now})
		if !ok || et != ClassificationOutage {
			t.Errorf("findCriticalAnomaly() expected Outage, got %v, %v", et, ok)
		}

		t.Run("Should NOT detect outage if some peers still see the prefix", func(t *testing.T) {
			s2 := &prefixStats{
				totalAnn:       0,
				totalWith:      3,
				uniquePeers:    map[string]bool{"p4": true, "p5": true}, // 2 peers still see it
				uniqueHosts:    map[string]bool{"h3": true},
				withdrawnPeers: map[string]bool{"p1": true, "p2": true, "p3": true},
				withdrawnHosts: map[string]bool{"h1": true, "h2": true},
			}
			et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s2, 65.0, &MessageContext{Now: now})
			if ok && et == ClassificationOutage {
				t.Errorf("findCriticalAnomaly() detected Outage but 2 peers still see the prefix!")
			}
		})
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

	t.Run("DDoS Mitigation Detection", func(t *testing.T) {
		// Mock seen DB for historical origin
		seenDBPath := filepath.Join(t.TempDir(), "test-seen-ddos.db")
		seenDB, _ := utils.OpenDiskTrie(seenDBPath)
		defer func() { _ = seenDB.Close() }()

		oldASN := uint32(1234)
		asnData := make([]byte, 4)
		binary.BigEndian.PutUint32(asnData, oldASN)
		_, ipNet, _ := net.ParseCIDR("1.1.1.0/24")
		_ = seenDB.Insert(ipNet, asnData)

		c.seenDB = seenDB

		s := &prefixStats{
			uniquePeers: map[string]bool{"p1": true},
			uniqueHosts: map[string]bool{"h1": true},
		}

		ctx := &MessageContext{
			OriginASN:      13335, // Cloudflare
			LastRpkiStatus: int32(utils.RPKIInvalidASN),
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, ctx)
		if !ok || et != ClassificationDDoSMitigation {
			t.Errorf("findCriticalAnomaly() expected DDoSMitigation, got %v, %v", et, ok)
		}
	})

	t.Run("DDoS Mitigation Self-Filter", func(t *testing.T) {
		// Mock seen DB where historical origin is SAME as provider
		seenDBPath := filepath.Join(t.TempDir(), "test-seen-ddos-self.db")
		seenDB, _ := utils.OpenDiskTrie(seenDBPath)
		defer func() { _ = seenDB.Close() }()

		providerASN := uint32(13335)
		asnData := make([]byte, 4)
		binary.BigEndian.PutUint32(asnData, providerASN)
		_, ipNet, _ := net.ParseCIDR("1.1.1.0/24")
		_ = seenDB.Insert(ipNet, asnData)

		c.seenDB = seenDB

		s := &prefixStats{
			uniquePeers: map[string]bool{"p1": true},
			uniqueHosts: map[string]bool{"h1": true},
		}

		ctx := &MessageContext{
			OriginASN:      providerASN, // Same as historical
			LastRpkiStatus: int32(utils.RPKIInvalidASN),
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, ctx)
		if ok || et == ClassificationDDoSMitigation {
			t.Errorf("findCriticalAnomaly() expected None for self-mitigation, got %v", et)
		}
	})

	t.Run("DDoS Mitigation Sibling Filter", func(t *testing.T) {
		// Mock ASN mapping with siblings
		asnMapping := utils.NewASNMapping()
		utils.SetASNOrgID(asnMapping, 13335, "CLOUDFLARE")
		utils.SetASNOrgID(asnMapping, 13336, "CLOUDFLARE")
		c.asnMapping = asnMapping

		// Mock seen DB where historical origin is SIBLING of provider
		seenDBPath := filepath.Join(t.TempDir(), "test-seen-ddos-sibling.db")
		seenDB, _ := utils.OpenDiskTrie(seenDBPath)
		defer func() { _ = seenDB.Close() }()

		oldASN := uint32(13336)
		asnData := make([]byte, 4)
		binary.BigEndian.PutUint32(asnData, oldASN)
		_, ipNet, _ := net.ParseCIDR("1.1.1.0/24")
		_ = seenDB.Insert(ipNet, asnData)

		c.seenDB = seenDB

		s := &prefixStats{
			uniquePeers: map[string]bool{"p1": true},
			uniqueHosts: map[string]bool{"h1": true},
		}

		ctx := &MessageContext{
			OriginASN:      13335, // Cloudflare
			LastRpkiStatus: int32(utils.RPKIInvalidASN),
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, ctx)
		if ok || et == ClassificationDDoSMitigation {
			t.Errorf("findCriticalAnomaly() expected None for sibling mitigation, got %v", et)
		}
	})
}
