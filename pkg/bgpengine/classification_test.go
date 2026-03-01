package bgpengine

import (
	"fmt"
	"testing"
	"time"
)

func runClassificationTest(t *testing.T, name string, expect Level2EventType, steps func(p *BGPProcessor, now time.Time)) {
	t.Run(name, func(t *testing.T) {
		var lastLevel2 Level2EventType
		onEvent := func(lat, lng float64, cc string, eventType EventType, level2Type Level2EventType, prefix string, asn uint32) {
			if level2Type != Level2None {
				lastLevel2 = level2Type
			}
		}
		p := NewBGPProcessor(func(uint32) (float64, float64, string) { return 0, 0, "US" }, nil, nil, nil, func(string) uint32 { return 0 }, onEvent)
		now := time.Now().Truncate(time.Hour)
		steps(p, now)
		if lastLevel2 != expect {
			t.Errorf("%s: expected %s, got %s", name, expect, lastLevel2)
		}
	})
}

func TestClassification(t *testing.T) {
	runClassificationTest(t, "Babbling", Level2Babbling, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 11; i++ {
			p.classifyEvent("1.1.1.0/24", &MessageContext{
				Peer: "peer1", PathStr: "[100 200]", Now: now.Add(time.Duration(i) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Discovery (100 peers)", Level2Discovery, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 100; i++ {
			p.classifyEvent("2.2.2.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Discovery (Catch-all)", Level2Discovery, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 30; i++ {
			p.classifyEvent("11.11.11.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i*5) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Link Flap", Level2LinkFlap, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 10; i++ {
			peer := fmt.Sprintf("peer%d", i%5)
			p.classifyEvent("3.3.3.0/24", &MessageContext{
				Peer: peer, IsWithdrawal: true, Now: now.Add(time.Duration(i*20) * time.Second),
			})
			p.classifyEvent("3.3.3.0/24", &MessageContext{
				Peer: peer, PathStr: "[100 200]", Now: now.Add(time.Duration(i*20+1) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Outage", Level2Outage, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 3; i++ {
			p.classifyEvent("4.4.4.0/24", &MessageContext{
				Peer: "peer1", IsWithdrawal: true, Now: now.Add(time.Duration(i*50) * time.Second),
			})
		}
		p.classifyEvent("4.4.4.0/24", &MessageContext{
			Peer: "peer1", IsWithdrawal: true, Now: now.Add(150 * time.Second),
		})
	})

	runClassificationTest(t, "Route Leak", Level2RouteLeak, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 5; i++ {
			p.classifyEvent("5.5.5.0/24", &MessageContext{
				Peer: "peer1", PathStr: "[3356 500 2914]", Now: now.Add(time.Duration(i*30) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Path Hunting", Level2PathHunting, func(p *BGPProcessor, now time.Time) {
		p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200]", PathLen: 2, Now: now})
		p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", IsWithdrawal: true, Now: now.Add(30 * time.Second)})
		p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300]", PathLen: 3, Now: now.Add(60 * time.Second)})
		p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400]", PathLen: 4, Now: now.Add(90 * time.Second)})
		p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400 500]", PathLen: 5, Now: now.Add(120 * time.Second)})
	})

	runClassificationTest(t, "Policy Churn", Level2PolicyChurn, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 6; i++ {
			p.classifyEvent("7.7.7.0/24", &MessageContext{
				Peer: "p1", PathStr: "[100]", CommStr: fmt.Sprintf("[[100:%d]]", i), Now: now.Add(time.Duration(i*25) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Path Length Oscillation", Level2PathLengthOscillation, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 4; i++ {
			p.classifyEvent("8.8.8.0/24", &MessageContext{
				Peer: "p1", PathStr: "[100 200]", PathLen: 2, Now: now.Add(time.Duration(i*60) * time.Second),
			})
			p.classifyEvent("8.8.8.0/24", &MessageContext{
				Peer: "p1", PathStr: "[100 200 300]", PathLen: 3, Now: now.Add(time.Duration(i*60+30) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Aggregator Flap", Level2AggFlap, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 20; i++ {
			peer := fmt.Sprintf("peer%d", i%5)
			p.classifyEvent("9.9.9.0/24", &MessageContext{
				Peer: peer, PathStr: "[100]", Aggregator: fmt.Sprintf("AS%d", i), Now: now.Add(time.Duration(i*5) * time.Second),
			})
		}
		p.classifyEvent("9.9.9.0/24", &MessageContext{
			Peer: "peer0", PathStr: "[100]", Aggregator: "AS-FINAL", Now: now.Add(130 * time.Second),
		})
	})

	runClassificationTest(t, "Next-Hop Flap", Level2NextHopOscillation, func(p *BGPProcessor, now time.Time) {
		for i := 0; i < 10; i++ {
			peer := fmt.Sprintf("peer%d", i%5)
			p.classifyEvent("10.10.10.0/24", &MessageContext{
				Peer: peer, PathStr: "[100]", NextHop: fmt.Sprintf("1.1.1.%d", i%2), Now: now.Add(time.Duration(i*10) * time.Second),
			})
		}
		p.classifyEvent("10.10.10.0/24", &MessageContext{
			Peer: "peer0", PathStr: "[100]", NextHop: "1.1.1.0", Now: now.Add(130 * time.Second),
		})
	})
}
