package bgpengine

import (
	"encoding/binary"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func runClassificationTest(t *testing.T, name string, expect ClassificationType, steps func(p *BGPProcessor, now time.Time, classify func(prefix string, ctx *MessageContext))) {
	t.Run(name, func(t *testing.T) {
		var lastClassification ClassificationType
		onEvent := func(lat, lng float64, cc, city string, eventType EventType, classificationType ClassificationType, prefix string, asn, historicalASN uint32, leakDetail ...*LeakDetail) {
			if classificationType != ClassificationNone {
				lastClassification = classificationType
			}
		}
		p := NewBGPProcessor(func(uint32) (float64, float64, string, string, geoservice.ResolutionType) {
			return 0, 0, "US", "New York", geoservice.ResGeoIP
		}, nil, nil, nil, nil, func(string) uint32 { return 0 }, time.Now, onEvent)
		now := time.Now().Truncate(time.Hour)

		classify := func(prefix string, ctx *MessageContext) {
			wIdx := int(utils.HashUint32(p.prefixToIP(prefix)) % uint32(len(p.workers)))
			if e, ok := p.workers[wIdx].classifier.ClassifyEvent(prefix, ctx); ok {
				if lat, lng, cc, city, _ := p.geo(e.IP); cc != "" {
					if e.LeakDetail != nil {
						p.onEvent(lat, lng, cc, city, e.EventType, e.ClassificationType, e.Prefix, e.ASN, e.HistoricalASN, e.LeakDetail)
					} else {
						p.onEvent(lat, lng, cc, city, e.EventType, e.ClassificationType, e.Prefix, e.ASN, e.HistoricalASN)
					}
				}
			}
		}

		steps(p, now, classify)
		if lastClassification != expect {
			t.Errorf("%s: expected %s, got %s", name, expect, lastClassification)
		}
	})
}

//nolint:gocognit // this is test code, so it's okay if it's a bit complex
func TestClassification(t *testing.T) {
	runClassificationTest(t, "Discovery (100 peers)", ClassificationDiscovery, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 100; i++ {
			classify("2.2.2.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Discovery (Catch-all)", ClassificationDiscovery, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 150; i++ {
			classify("11.11.11.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i*5) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Link Flap", ClassificationLinkFlap, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 10; i++ {
			peer := fmt.Sprintf("peer%d", i%5)
			classify("3.3.3.0/24", &MessageContext{
				Peer: peer, IsWithdrawal: true, Now: now.Add(time.Duration(i*20) * time.Second),
			})
			classify("3.3.3.0/24", &MessageContext{
				Peer: peer, PathStr: "[100 200]", Now: now.Add(time.Duration(i*20+1) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Outage", ClassificationOutage, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 10; i++ {
			classify("4.4.4.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), Host: fmt.Sprintf("h%d", i%3), IsWithdrawal: true, Now: now.Add(time.Duration(i*50) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Route Leak", ClassificationRouteLeak, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 5; i++ {
			classify("5.5.5.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), Host: fmt.Sprintf("rrc%d", i%2),
				PathStr: "[3356 500 2914]", PathLen: 11, Now: now.Add(time.Duration(i*30) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Path Hunting", ClassificationPathHunting, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200]", PathLen: 2, Now: now})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", IsWithdrawal: true, Now: now.Add(30 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300]", PathLen: 3, Now: now.Add(60 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400]", PathLen: 4, Now: now.Add(90 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400 500]", PathLen: 5, Now: now.Add(120 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400 500 600]", PathLen: 6, Now: now.Add(150 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400 500 600 700]", PathLen: 7, Now: now.Add(180 * time.Second)})
	})

	runClassificationTest(t, "DDoS Mitigation", ClassificationDDoSMitigation, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		prefix := "7.7.7.0/24"

		// Mock historical origin in seenDB
		seenDBPath := filepath.Join(t.TempDir(), "test-seen-ddos-main.db")
		seenDB, _ := utils.OpenDiskTrie(seenDBPath)
		defer func() { _ = seenDB.Close() }()
		oldASN := uint32(1234)
		asnData := make([]byte, 4)
		binary.BigEndian.PutUint32(asnData, oldASN)
		_, ipNet, _ := net.ParseCIDR(prefix)
		_ = seenDB.Insert(ipNet, asnData)

		for i := range p.workers {
			p.workers[i].classifier.seenDB = seenDB
		}

		// Mock a historical origin by sending a valid announcement first
		for i := range 10 {
			classify(prefix, &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), Host: fmt.Sprintf("rrc%d", i%3),
				OriginASN:      1234,
				LastRpkiStatus: int32(utils.RPKIValid),
				Now:            now.Add(time.Duration(i*30) * time.Second),
			})
		}

		// Now send the DDoS mitigation announcements
		for i := range 10 {
			host := fmt.Sprintf("rrc%d", i%3)
			classify(prefix, &MessageContext{
				Peer: fmt.Sprintf("peer%d", i+10), Host: host,
				OriginASN:      13335, // Cloudflare (DDoS Provider)
				LastRpkiStatus: int32(utils.RPKIInvalidASN),
				Now:            now.Add(10*time.Minute + time.Duration(i*30)*time.Second),
			})
		}

		// Verify leak details (Provider/Impacted)
		finalState, _ := p.workers[int(utils.HashUint32(p.prefixToIP(prefix))%uint32(len(p.workers)))].classifier.GetPrefixState(prefix)
		if finalState.LeakerAsn != 13335 {
			t.Errorf("expected Provider AS13335, got AS%d", finalState.LeakerAsn)
		}
		if finalState.VictimAsn != 1234 {
			t.Errorf("expected Impacted AS1234, got AS%d", finalState.VictimAsn)
		}
	})

	t.Run("DDoS Mitigation Re-Emission", func(t *testing.T) {
		prefix := "8.8.8.0/24"

		// Mock historical origin in seenDB
		seenDBPath := filepath.Join(t.TempDir(), "test-seen-ddos-re-emission.db")
		seenDB, _ := utils.OpenDiskTrie(seenDBPath)
		defer func() { _ = seenDB.Close() }()
		oldASN := uint32(1234)
		asnData := make([]byte, 4)
		binary.BigEndian.PutUint32(asnData, oldASN)
		_, ipNet, _ := net.ParseCIDR(prefix)
		_ = seenDB.Insert(ipNet, asnData)

		p := NewBGPProcessor(func(uint32) (float64, float64, string, string, geoservice.ResolutionType) {
			return 0, 0, "", "", geoservice.ResUnknown
		}, seenDB, nil, nil, nil, func(p string) uint32 {
			ip, _, _ := net.ParseCIDR(p)
			if ip == nil {
				return 0
			}
			return binary.BigEndian.Uint32(ip.To4())
		}, time.Now, func(lat, lng float64, cc, city string, eventType EventType, ct ClassificationType, prefix string, asn, historicalASN uint32, ld ...*LeakDetail) {
		})

		now := time.Now()

		// 1. Initially classify as DDoS Mitigation (need multiple peers for consensus)
		for i := 0; i < 10; i++ {
			p.handleAnnouncements(p.workers[0], []RISAnnouncement{{Prefixes: []string{prefix}}}, &MessageContext{
				Peer: fmt.Sprintf("p%d", i), Host: "h1", OriginASN: 13335, LastRpkiStatus: int32(utils.RPKIInvalidASN), Now: now.Add(time.Duration(i) * time.Second), LastOriginAsn: 1234,
			}, now.Add(time.Duration(i)*time.Second), 13335)
		}

		// 2. Send another announcement for the same prefix - it should be re-emitted with LeakDetail
		events := p.handleAnnouncements(p.workers[0], []RISAnnouncement{{Prefixes: []string{prefix}}}, &MessageContext{
			Peer: "p2", Host: "h1", OriginASN: 13335, LastRpkiStatus: int32(utils.RPKIInvalidASN), Now: now.Add(10 * time.Second), LastOriginAsn: 1234,
		}, now.Add(10*time.Second), 13335)

		found := false
		for _, e := range events {
			if e.ClassificationType == ClassificationDDoSMitigation {
				if e.LeakDetail == nil {
					t.Fatalf("Expected LeakDetail in re-emitted DDoS Mitigation event")
				}
				if e.LeakDetail.LeakerASN != 13335 {
					t.Errorf("Expected LeakerASN 13335, got %d", e.LeakDetail.LeakerASN)
				}
				if e.LeakDetail.VictimASN != 1234 {
					t.Errorf("Expected VictimASN 1234, got %d", e.LeakDetail.VictimASN)
				}
				found = true
			}
		}
		if !found {
			t.Errorf("DDoS Mitigation event not re-emitted")
		}
	})
}
