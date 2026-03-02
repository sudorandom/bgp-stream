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
		}, nil, nil, nil, func(string) uint32 { return 0 }, onEvent)
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
				Peer: "peer1", PathStr: "[3356 500 2914]", Now: now.Add(time.Duration(i*30) * time.Second),
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
