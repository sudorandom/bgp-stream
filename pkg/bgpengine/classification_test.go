package bgpengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/geoservice"
)

func runClassificationTest(t *testing.T, name string, expect Level2EventType, steps func(p *BGPProcessor, now time.Time, classify func(prefix string, ctx *MessageContext))) {
	t.Run(name, func(t *testing.T) {
		var lastLevel2 Level2EventType
		onEvent := func(lat, lng float64, cc string, eventType EventType, level2Type Level2EventType, prefix string, asn uint32) {
			if level2Type != Level2None {
				lastLevel2 = level2Type
			}
		}
		p := NewBGPProcessor(func(uint32) (float64, float64, string, geoservice.ResolutionType) {
			return 0, 0, "US", geoservice.ResGeoIP
		}, nil, nil, nil, nil, func(string) uint32 { return 0 }, onEvent)
		now := time.Now().Truncate(time.Hour)

		classify := func(prefix string, ctx *MessageContext) {
			if e, ok := p.classifyEvent(prefix, ctx); ok {
				if lat, lng, cc, _ := p.geo(e.ip); cc != "" {
					p.onEvent(lat, lng, cc, e.eventType, e.level2Type, e.prefix, e.asn)
				}
			}
		}

		steps(p, now, classify)
		if lastLevel2 != expect {
			t.Errorf("%s: expected %s, got %s", name, expect, lastLevel2)
		}
	})
}

func TestClassification(t *testing.T) {
	runClassificationTest(t, "Babbling", Level2Babbling, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 21; i++ {
			classify("1.1.1.0/24", &MessageContext{
				Peer: "peer1", PathStr: "[100 200]", Now: now.Add(time.Duration(i) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Discovery (100 peers)", Level2Discovery, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 100; i++ {
			classify("2.2.2.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Discovery (Catch-all)", Level2Discovery, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 101; i++ {
			classify("11.11.11.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i*5) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Link Flap", Level2LinkFlap, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
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

	runClassificationTest(t, "Outage", Level2Outage, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 3; i++ {
			classify("4.4.4.0/24", &MessageContext{
				Peer: "peer1", IsWithdrawal: true, Now: now.Add(time.Duration(i*50) * time.Second),
			})
		}
		classify("4.4.4.0/24", &MessageContext{
			Peer: "peer1", IsWithdrawal: true, Now: now.Add(150 * time.Second),
		})
	})

	runClassificationTest(t, "Route Leak", Level2RouteLeak, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 5; i++ {
			classify("5.5.5.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), Host: fmt.Sprintf("rrc%d", i%2),
				PathStr: "[3356 500 2914]", Now: now.Add(time.Duration(i*30) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Path Hunting", Level2PathHunting, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200]", PathLen: 2, Now: now})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", IsWithdrawal: true, Now: now.Add(30 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300]", PathLen: 3, Now: now.Add(60 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400]", PathLen: 4, Now: now.Add(90 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400 500]", PathLen: 5, Now: now.Add(120 * time.Second)})
		classify("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400 500 600]", PathLen: 6, Now: now.Add(150 * time.Second)})
	})

	runClassificationTest(t, "Policy Churn", Level2PolicyChurn, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 11; i++ {
			classify("7.7.7.0/24", &MessageContext{
				Peer: "p1", PathStr: "[100]", CommStr: fmt.Sprintf("[[100:%d]]", i), Now: now.Add(time.Duration(i*25) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Path Length Oscillation", Level2PathLengthOscillation, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 4; i++ {
			classify("8.8.8.0/24", &MessageContext{
				Peer: "p1", PathStr: "[100 200]", PathLen: 2, Now: now.Add(time.Duration(i*60) * time.Second),
			})
			classify("8.8.8.0/24", &MessageContext{
				Peer: "p1", PathStr: "[100 200 300]", PathLen: 3, Now: now.Add(time.Duration(i*60+30) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Aggregator Flap", Level2AggFlap, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 20; i++ {
			peer := fmt.Sprintf("peer%d", i%5)
			classify("9.9.9.0/24", &MessageContext{
				Peer: peer, PathStr: "[100]", Aggregator: fmt.Sprintf("AS%d", i), Now: now.Add(time.Duration(i*5) * time.Second),
			})
		}
		classify("9.9.9.0/24", &MessageContext{
			Peer: "peer0", PathStr: "[100]", Aggregator: "AS-FINAL", Now: now.Add(130 * time.Second),
		})
	})

	runClassificationTest(t, "Next-Hop Flap", Level2NextHopOscillation, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 15; i++ {
			classify("10.10.10.0/24", &MessageContext{
				Peer: "peer1", PathStr: "[100]", NextHop: fmt.Sprintf("1.1.1.%d", i%2), Now: now.Add(time.Duration(i*10) * time.Second),
			})
		}
		// Second peer to trigger len(uniqueHops) > 1
		classify("10.10.10.0/24", &MessageContext{
			Peer: "peer2", PathStr: "[100]", NextHop: "2.2.2.2", Now: now.Add(160 * time.Second),
		})
	})
}

func TestBGPClassifier_IsDDoSProvider(t *testing.T) {
	c := NewBGPClassifier(nil, nil)

	tests := []struct {
		asn      uint32
		expected bool
	}{
		{13335, true},  // Cloudflare
		{15169, true},  // Google
		{12345, false}, // Random
		{20940, true},  // Akamai
		{32934, true},  // Facebook
		{6453, true},   // Tata
		{99999, false}, // Random
	}

	for _, tt := range tests {
		result := c.isDDoSProvider(tt.asn)
		if result != tt.expected {
			t.Errorf("expected isDDoSProvider(%d) to be %v, got %v", tt.asn, tt.expected, result)
		}
	}
}

func TestBGPClassifier_HasRouteLeak(t *testing.T) {
	c := NewBGPClassifier(nil, nil)

	tests := []struct {
		name     string
		pathStr  string
		expected bool
	}{
		{
			name:     "Empty path",
			pathStr:  "",
			expected: false,
		},
		{
			name:     "Short path",
			pathStr:  "[100 200]",
			expected: false,
		},
		{
			name: "Valley-free violation (Tier-1 -> Stub -> Tier-1)",
			// 1299 (Telia, Tier-1) -> 500 (Stub) -> 3356 (Level 3, Tier-1)
			pathStr:  "[1299 500 3356]",
			expected: true,
		},
		{
			name: "Valid path (Stub -> Tier-1 -> Tier-1)",
			// 500 (Stub) -> 1299 (Telia) -> 3356 (Level 3)
			pathStr:  "[500 1299 3356]",
			expected: false,
		},
		{
			name: "Cloud provider in middle (Not a leak)",
			// 1299 (Tier-1) -> 15169 (Google, Cloud) -> 3356 (Tier-1)
			pathStr:  "[1299 15169 3356]",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &MessageContext{
				PathStr: tt.pathStr,
			}
			_, leak := c.hasRouteLeak(ctx)
			if leak != tt.expected {
				t.Errorf("expected %v, got %v for path %s", tt.expected, leak, tt.pathStr)
			}
		})
	}
}

func TestBGPClassifier_IsTier1(t *testing.T) {
	c := NewBGPClassifier(nil, nil)
	tests := []struct {
		asn      uint32
		expected bool
	}{
		{209, true},
		{701, true},
		{1299, true},
		{3356, true},
		{6453, true},
		{174, true},
		{6939, true},
		{12345, false},
		{99999, false},
	}
	for _, tt := range tests {
		result := c.isTier1(tt.asn)
		if result != tt.expected {
			t.Errorf("expected isTier1(%d) to be %v, got %v", tt.asn, tt.expected, result)
		}
	}
}

func TestBGPClassifier_IsCloud(t *testing.T) {
	c := NewBGPClassifier(nil, nil)
	tests := []struct {
		asn      uint32
		expected bool
	}{
		{13335, true},
		{15169, true},
		{16509, true},
		{14618, true},
		{8075, true},
		{12345, false},
		{99999, false},
	}
	for _, tt := range tests {
		result := c.isCloud(tt.asn)
		if result != tt.expected {
			t.Errorf("expected isCloud(%d) to be %v, got %v", tt.asn, tt.expected, result)
		}
	}
}
