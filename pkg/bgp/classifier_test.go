package bgp

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"path/filepath"

	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgp/proto/v1"
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

func TestClassifier_FindCriticalAnomaly_Outage(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil, time.Now)
	now := time.Now()

	t.Run("Outage Detection", func(t *testing.T) {
		s := &prefixStats{
			totalWith:      30,
			totalAnn:       0,
			withdrawnPeers: make(map[string]bool),
			withdrawnHosts: map[string]bool{"h1": true, "h2": true, "h3": true, "h4": true, "h5": true},
			uniquePeers:    map[string]bool{},
			uniqueHosts:    map[string]bool{},
		}
		for i := 1; i <= 20; i++ {
			s.withdrawnPeers[fmt.Sprintf("p%d", i)] = true
		}
		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, &MessageContext{Now: now})
		if !ok || et != ClassificationOutage {
			t.Errorf("findCriticalAnomaly() expected Outage, got %v, %v", et, ok)
		}

		t.Run("Should correctly detect outage for small prefixes (<=2 peers)", func(t *testing.T) {
			sSmall := &prefixStats{
				totalWith:      5,
				totalAnn:       0,
				withdrawnPeers: map[string]bool{"p1": true, "p2": true},
				withdrawnHosts: map[string]bool{"h1": true, "h2": true},
				uniquePeers:    map[string]bool{},
				uniqueHosts:    map[string]bool{},
			}
			et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", sSmall, 65.0, &MessageContext{Now: now})
			if !ok || et != ClassificationOutage {
				t.Errorf("findCriticalAnomaly() expected Outage for small prefix, got %v, %v", et, ok)
			}
		})

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
}

func TestClassifier_FindCriticalAnomaly_Hijack(t *testing.T) {
	now := time.Now()

	t.Run("Hijack High Signal Detection", func(t *testing.T) {
		c := NewClassifier(nil, nil, nil, nil, nil, nil, time.Now)
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
		if !ok || et != ClassificationHijack {
			t.Errorf("findCriticalAnomaly() expected BGP Hijack, got %v, %v", et, ok)
		}
	})
}

func TestClassifier_FindCriticalAnomaly_DDoS(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil, time.Now)
	now := time.Now()

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
			CommStr:        "65535:666",
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, ctx)
		if !ok || et != ClassificationDDoSMitigation {
			if et != ClassificationDDoSMitigation {
				t.Errorf("findCriticalAnomaly() expected DDoSMitigation, got %v, %v", et, ok)
			}
		}
	})

	t.Run("DDoS Mitigation Victim Identification via LPM", func(t *testing.T) {
		// Mock seen DB with a /24 but NOT the specific /32
		seenDBPath := filepath.Join(t.TempDir(), "test-seen-ddos-lpm.db")
		seenDB, _ := utils.OpenDiskTrie(seenDBPath)
		defer func() { _ = seenDB.Close() }()

		victimASN := uint32(100)
		asnData := make([]byte, 4)
		binary.BigEndian.PutUint32(asnData, victimASN)
		_, ipNet, _ := net.ParseCIDR("1.1.1.0/24")
		_ = seenDB.Insert(ipNet, asnData)

		c := NewClassifier(seenDB, nil, nil, nil, func(p string) uint32 {
			ip, _, _ := net.ParseCIDR(p)
			return binary.BigEndian.Uint32(ip.To4())
		}, utils.NewLRUCache[string, *bgpproto.PrefixState](100), time.Now)

		ctx := &MessageContext{
			OriginASN: 200,
			CommStr:   "65535:666",
			Now:       time.Now(),
		}

		// Classify a /32 that is part of the /24
		prefix := "1.1.1.1/32"
		var event PendingEvent
		var ok bool
		for i := 0; i < 6; i++ {
			event, ok = c.ClassifyEvent(prefix, ctx)
		}

		if !ok {
			t.Fatalf("Expected classification, got none")
		}

		if event.LeakDetail == nil {
			t.Fatalf("Expected LeakDetail, got nil")
		}

		if event.LeakDetail.VictimASN != victimASN {
			t.Errorf("Expected VictimASN %d (via LPM), got %d", victimASN, event.LeakDetail.VictimASN)
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
			CommStr:        "65535:666",
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, ctx)
		if !ok || et != ClassificationDDoSMitigation {
			if et != ClassificationDDoSMitigation {
				t.Errorf("findCriticalAnomaly() expected DDoS Mitigation for self-mitigation, got %v", et)
			}
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
			CommStr:        "65535:666",
			Now:            now,
		}

		et, _, ok := c.findCriticalAnomaly("1.1.1.0/24", s, 65.0, ctx)
		if !ok || et != ClassificationDDoSMitigation {
			if et != ClassificationDDoSMitigation {
				t.Errorf("findCriticalAnomaly() expected DDoS Mitigation for sibling mitigation, got %v", et)
			}
		}
	})
}

func TestClassifier_OutageRecovery(t *testing.T) {
	now := time.Now()
	prefix := "1.1.1.0/24"
	c := NewClassifier(nil, nil, nil, nil, func(p string) uint32 { return 0 }, utils.NewLRUCache[string, *bgpproto.PrefixState](100), func() time.Time { return now })

	// 1. Simulate Outage
	ctx := &MessageContext{
		IsWithdrawal: true,
		Host:         "h1",
		Peer:         "p1",
		Now:          now,
	}

	// Send enough withdrawals to trigger outage (need multiple messages and time elapsed)
	for i := 0; i < 10; i++ {
		now = now.Add(10 * time.Second)
		ctx.Now = now
		_, _ = c.ClassifyEvent(prefix, ctx)
	}

	state, _ := c.GetPrefixState(prefix)
	state.ClassifiedType = int32(ClassificationOutage)
	state.ClassifiedTimeTs = now.Unix()

	// Verify it's an outage
	if ClassificationType(state.ClassifiedType) != ClassificationOutage {
		t.Fatalf("Expected state to be Outage, got %v", ClassificationType(state.ClassifiedType))
	}

	// 2. Send Announcement (Recovery)
	now = now.Add(10 * time.Second)
	ctx.Now = now
	ctx.IsWithdrawal = false
	ctx.OriginASN = 1234

	c.ClassifyEvent(prefix, ctx)
	// It might not be classified as anything immediately after recovery,
	// but the important thing is that the Outage state is cleared.

	state, _ = c.GetPrefixState(prefix)
	if ClassificationType(state.ClassifiedType) == ClassificationOutage {
		t.Errorf("Expected Outage state to be cleared after announcement, but it's still %v", ClassificationType(state.ClassifiedType))
	}
}

func TestClassifier_FindCriticalAnomaly_DDoS_Detailed(t *testing.T) {
	c := NewClassifier(nil, nil, nil, nil, nil, nil, time.Now)
	now := time.Now()

	tests := []struct {
		name         string
		prefix       string
		commStr      string
		pathStr      string
		wantType     ClassificationType
		wantLeakType LeakType
	}{
		{
			name:         "Flowspec via traffic-rate community",
			prefix:       "1.1.1.0/24",
			commStr:      "traffic-rate: 0",
			wantType:     ClassificationDDoSMitigation,
			wantLeakType: DDoSFlowspec,
		},
		{
			name:         "Flowspec via traffic-action community",
			prefix:       "1.1.1.0/24",
			commStr:      "65000:100 [traffic-action: sample]",
			wantType:     ClassificationDDoSMitigation,
			wantLeakType: DDoSFlowspec,
		},
		{
			name:         "RTBH via community",
			prefix:       "1.1.1.0/24",
			commStr:      "65535:666",
			wantType:     ClassificationDDoSMitigation,
			wantLeakType: DDoSRTBH,
		},
		{
			name:         "RTBH via /32 IPv4",
			prefix:       "1.1.1.1/32",
			commStr:      "",
			wantType:     ClassificationDDoSMitigation,
			wantLeakType: DDoSRTBH,
		},
		{
			name:         "RTBH via /128 IPv6",
			prefix:       "2001:db8::1/128",
			commStr:      "",
			wantType:     ClassificationDDoSMitigation,
			wantLeakType: DDoSRTBH,
		},
		{
			name:         "Traffic Redirection via Scrubbing Center ASN (Prolexic) - Prepending",
			prefix:       "1.1.1.0/24",
			pathStr:      "[1234 19324 5678 5678 5678]",
			wantType:     ClassificationDDoSMitigation,
			wantLeakType: DDoSTrafficRedirection,
		},
		{
			name:         "Traffic Redirection via Scrubbing Center ASN (Imperva) - Origin Takeover",
			prefix:       "1.1.1.0/24",
			pathStr:      "[1234 19551]",
			wantType:     ClassificationDDoSMitigation,
			wantLeakType: DDoSTrafficRedirection,
		},
		{
			name:         "Traffic Redirection via Scrubbing Center ASN (Radware) - Prepending",
			prefix:       "1.1.1.0/24",
			pathStr:      "[100 200 6428 400 400]",
			wantType:     ClassificationDDoSMitigation,
			wantLeakType: DDoSTrafficRedirection,
		},
		{
			name:         "No DDoS Mitigation",
			prefix:       "1.1.1.0/24",
			commStr:      "65000:100",
			pathStr:      "[100 200 300]",
			wantType:     ClassificationNone,
			wantLeakType: LeakUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &prefixStats{
				uniquePeers: map[string]bool{"p1": true},
				uniqueHosts: map[string]bool{"h1": true},
			}

			ctx := &MessageContext{
				OriginASN: 13335,
				CommStr:   tt.commStr,
				PathStr:   tt.pathStr,
				Now:       now,
			}

			// Create a temporary mock for historical ASN
			seenDBPath := filepath.Join(t.TempDir(), "test-seen-ddos-redirect.db")
			seenDB, _ := utils.OpenDiskTrie(seenDBPath)
			defer func() { _ = seenDB.Close() }()

			oldASN := uint32(5678) // The victim
			asnData := make([]byte, 4)
			binary.BigEndian.PutUint32(asnData, oldASN)
			_, ipNet, _ := net.ParseCIDR("1.1.1.0/24")
			_ = seenDB.Insert(ipNet, asnData)

			c.seenDB = seenDB

			gotType, gotLD, ok := c.findCriticalAnomaly(tt.prefix, s, 65.0, ctx)

			if tt.wantType == ClassificationNone {
				if ok && gotType == ClassificationDDoSMitigation {
					t.Errorf("Expected no DDoS mitigation, got %v", gotType)
				}
				return
			}

			if !ok || gotType != tt.wantType {
				t.Errorf("Expected type %v, got %v (ok=%v)", tt.wantType, gotType, ok)
			}

			if gotLD == nil {
				t.Fatalf("Expected LeakDetail, got nil")
			}

			if gotLD.Type != tt.wantLeakType {
				t.Errorf("Expected subtype %v, got %v", tt.wantLeakType, gotLD.Type)
			}
		})
	}
}
