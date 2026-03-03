package bgpengine

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func TestHijackDetection(t *testing.T) {
	dbPath := "test-rpki-hijack.db"
	_ = os.RemoveAll(dbPath)
	defer func() {
		_ = os.RemoveAll(dbPath)
	}()

	rpki, err := utils.NewRPKIManager(dbPath)
	if err != nil {
		t.Fatalf("Failed to create RPKIManager: %v", err)
	}
	defer func() {
		_ = rpki.Close()
	}()

	// Setup mock VRP for 1.1.1.0/24 owned by AS100
	vrp := []utils.VRP{{Prefix: "1.1.1.0/24", MaxLength: 24, ASN: 100}}
	b, _ := json.Marshal(vrp)
	// Directly insert into the trie since we don't have a mock Sync
	if err := utils.SetVRPInTrie(rpki, "1.1.1.0/24", b); err != nil {
		t.Fatalf("Failed to set mock VRP: %v", err)
	}

	now := time.Now().Truncate(time.Hour)

	type testCase struct {
		name       string
		prefix     string
		updates    []*MessageContext
		expectAnom ClassificationType
		setup      func(p *BGPProcessor)
	}

	tests := []testCase{
		{
			name:   "Legitimate Announcement (Valid)",
			prefix: "1.1.1.0/24",
			updates: []*MessageContext{
				{Peer: "p1", Host: "rrc00", OriginASN: 100, Now: now},
				{Peer: "p2", Host: "rrc00", OriginASN: 100, Now: now.Add(time.Second)},
				{Peer: "p3", Host: "rrc01", OriginASN: 100, Now: now.Add(2 * time.Second)},
			},
			expectAnom: ClassificationNone,
		},
		{
			name:   "Single Peer RPKI Invalid (Noise/Leak)",
			prefix: "1.1.1.0/24",
			updates: []*MessageContext{
				{Peer: "p1", Host: "rrc00", OriginASN: 666, Now: now},
			},
			expectAnom: ClassificationNone,
		},
		{
			name:   "Transition Hijack (High Signal)",
			prefix: "1.1.1.0/24",
			updates: []*MessageContext{
				{Peer: "p1", Host: "rrc00", OriginASN: 666, Now: now},
				{Peer: "p2", Host: "rrc00", OriginASN: 666, Now: now.Add(time.Second)},
				{Peer: "p3", Host: "rrc01", OriginASN: 666, Now: now.Add(2 * time.Second)},
				{Peer: "p4", Host: "rrc01", OriginASN: 666, Now: now.Add(3 * time.Second)},
				{Peer: "p5", Host: "rrc02", OriginASN: 666, Now: now.Add(4 * time.Second)},
			},
			expectAnom: ClassificationRouteLeak,
			setup: func(p *BGPProcessor) {
				// Prefix was previously seen as AS100
				asnBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(asnBytes, 100)
				_ = p.seenDB.BatchInsertRaw(map[string][]byte{"1.1.1.0/24": asnBytes})
			},
		},
		{
			name:   "Multi-Peer Single-Host RPKI Invalid (Suppressed)",
			prefix: "1.1.1.0/24",
			updates: []*MessageContext{
				{Peer: "p1", Host: "rrc00", OriginASN: 666, Now: now},
				{Peer: "p2", Host: "rrc00", OriginASN: 666, Now: now.Add(time.Second)},
				{Peer: "p3", Host: "rrc00", OriginASN: 666, Now: now.Add(2 * time.Second)},
				{Peer: "p4", Host: "rrc00", OriginASN: 666, Now: now.Add(3 * time.Second)},
				{Peer: "p5", Host: "rrc00", OriginASN: 666, Now: now.Add(4 * time.Second)},
			},
			expectAnom: ClassificationNone, // Fails because all from same host
			setup: func(p *BGPProcessor) {
				asnBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(asnBytes, 100)
				_ = p.seenDB.BatchInsertRaw(map[string][]byte{"1.1.1.0/24": asnBytes})
			},
		},
		{
			name:   "Historical Origin Match (Suppressed)",
			prefix: "1.1.1.0/24",
			updates: []*MessageContext{
				{Peer: "p1", Host: "rrc00", OriginASN: 666, Now: now},
				{Peer: "p2", Host: "rrc00", OriginASN: 666, Now: now.Add(time.Second)},
				{Peer: "p3", Host: "rrc01", OriginASN: 666, Now: now.Add(2 * time.Second)},
				{Peer: "p4", Host: "rrc01", OriginASN: 666, Now: now.Add(3 * time.Second)},
				{Peer: "p5", Host: "rrc02", OriginASN: 666, Now: now.Add(4 * time.Second)},
			},
			expectAnom: ClassificationNone, // Fails because it was already seen as AS666
			setup: func(p *BGPProcessor) {
				asnBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(asnBytes, 666)
				_ = p.seenDB.BatchInsertRaw(map[string][]byte{"1.1.1.0/24": asnBytes})
			},
		},
		{
			name:   "New Prefix RPKI Invalid (High evidence requirement)",
			prefix: "9.9.9.0/24",
			updates: []*MessageContext{
				{Peer: "p1", Host: "rrc00", OriginASN: 999, Now: now},
				{Peer: "p2", Host: "rrc00", OriginASN: 999, Now: now.Add(time.Second)},
				{Peer: "p3", Host: "rrc01", OriginASN: 999, Now: now.Add(2 * time.Second)},
				{Peer: "p4", Host: "rrc01", OriginASN: 999, Now: now.Add(3 * time.Second)},
			},
			expectAnom: ClassificationNone,
		},
		{
			name:   "Sibling ASN Leak (Suppressed)",
			prefix: "1.1.1.0/24",
			updates: []*MessageContext{
				// Lumen (3356) -> Telstra Domestic (1221) -> Telstra International (4637)
				{Peer: "p1", Host: "rrc00", PathStr: "[3356 1221 4637]", Now: now},
				{Peer: "p2", Host: "rrc00", PathStr: "[3356 1221 4637]", Now: now.Add(time.Second)},
				{Peer: "p3", Host: "rrc01", PathStr: "[3356 1221 4637]", Now: now.Add(2 * time.Second)},
			},
			expectAnom: ClassificationNone,
			setup: func(p *BGPProcessor) {
				// Mock siblings
				p.asnMapping = utils.NewASNMapping() // Fresh mapping
				utils.SetASNOrgID(p.asnMapping, 1221, "TELSTRA")
				utils.SetASNOrgID(p.asnMapping, 4637, "TELSTRA")
			},
		},
		{
			name:   "Custom Sibling Association (Suppressed)",
			prefix: "117.25.105.0/24",
			updates: []*MessageContext{
				// Arelion (1299) -> CTGNet (23764) -> CN2 (4809)
				{Peer: "p1", Host: "rrc00", PathStr: "[1299 23764 4809]", Now: now},
				{Peer: "p2", Host: "rrc00", PathStr: "[1299 23764 4809]", Now: now.Add(time.Second)},
				{Peer: "p3", Host: "rrc01", PathStr: "[1299 23764 4809]", Now: now.Add(2 * time.Second)},
			},
			expectAnom: ClassificationNone,
			setup: func(p *BGPProcessor) {
				// Use the actual Load() which now includes hardcoded China Telecom siblings
				p.asnMapping = utils.NewASNMapping()
				_ = p.asnMapping.Load()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lastAnom ClassificationType
			onEvent := func(lat, lng float64, cc string, eventType EventType, classificationType ClassificationType, prefix string, asn uint32) {
				if classificationType != ClassificationNone {
					lastAnom = classificationType
				}
			}

			seenDBPath := "test-seen-hijack.db"
			_ = os.RemoveAll(seenDBPath)
			seenDB, _ := utils.OpenDiskTrie(seenDBPath)
			defer func() {
				_ = seenDB.Close()
				_ = os.RemoveAll(seenDBPath)
			}()

			p := NewBGPProcessor(func(uint32) (float64, float64, string, geoservice.ResolutionType) {
				return 0, 0, "US", geoservice.ResGeoIP
			}, seenDB, nil, nil, rpki, func(string) uint32 { return 0 }, onEvent)

			if tt.setup != nil {
				tt.setup(p)
			}

			for _, ctx := range tt.updates {
				if e, ok := p.classifier.classifyEvent(tt.prefix, ctx); ok {
					p.onEvent(0, 0, "US", e.eventType, e.classificationType, e.prefix, e.asn)
				}
			}

			if lastAnom != tt.expectAnom {
				t.Errorf("%s: expected %s, got %s", tt.name, tt.expectAnom, lastAnom)
			}
		})
	}
}
