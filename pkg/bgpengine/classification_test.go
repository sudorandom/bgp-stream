package bgpengine

import (
	"fmt"
	"testing"
	"time"
)

func TestClassification(t *testing.T) {
	tests := []struct {
		name             string
		steps            []func(p *BGPProcessor, now time.Time)
		expectLevel2     Level2EventType
	}{
		{
			name: "Babbling (1 peer, many identical updates)",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					for i := 0; i < 11; i++ {
						p.classifyEvent("1.1.1.0/24", &MessageContext{
							Peer: "peer1", PathStr: "[100 200]", Now: now.Add(time.Duration(i) * time.Second),
						})
					}
				},
			},
			expectLevel2: Level2Babbling,
		},
		{
			name: "Discovery (100 different peers, 1 update each)",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					for i := 0; i < 100; i++ {
						p.classifyEvent("2.2.2.0/24", &MessageContext{
							Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i) * time.Second),
						})
					}
				},
			},
			expectLevel2: Level2Discovery,
		},
		{
			name: "Discovery (Catch-all for high volume: 30 peers, 1 update each)",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					for i := 0; i < 30; i++ {
						p.classifyEvent("11.11.11.0/24", &MessageContext{
							Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i*5) * time.Second),
						})
					}
				},
			},
			expectLevel2: Level2Discovery,
		},
		{
			name: "Link Flap (Many withdrawals relative to announcements)",
			steps: []func(p *BGPProcessor, now time.Time) {
				func(p *BGPProcessor, now time.Time) {
					// Need > 5 withdrawals and ann/with < 2.5
					// Let's do 10 cycles of withdrawal + announcement
					// totalMsgs will be 20, perPeerRate will be 20 (if 1 peer)
					// We need to make sure it doesn't match Babbling (perPeerRate >= 5.0 && totalMsgs >= 10)
					// So let's use 5 peers to keep perPeerRate low
					for i := 0; i < 10; i++ {
						peer := fmt.Sprintf("peer%d", i%5)
						p.classifyEvent("3.3.3.0/24", &MessageContext{
							Peer: peer, IsWithdrawal: true, Now: now.Add(time.Duration(i*20) * time.Second),
						})
						p.classifyEvent("3.3.3.0/24", &MessageContext{
							Peer: peer, PathStr: "[100 200]", Now: now.Add(time.Duration(i*20+1) * time.Second),
						})
					}
				},
			},
			expectLevel2: Level2LinkFlap,
		},
		{
			name: "Outage (3+ withdrawals, 0 announcements)",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					// Need at least 120s elapsed for classification to trigger
					// and > 60s for Outage specifically
					for i := 0; i < 3; i++ {
						p.classifyEvent("4.4.4.0/24", &MessageContext{
							Peer: "peer1", IsWithdrawal: true, Now: now.Add(time.Duration(i*50) * time.Second),
						})
					}
					// Add a final dummy message to trigger evaluation after enough time has passed
					p.classifyEvent("4.4.4.0/24", &MessageContext{
						Peer: "peer1", IsWithdrawal: true, Now: now.Add(150 * time.Second),
					})
				},
			},
			expectLevel2: Level2Outage,
		},
		{
			name: "Route Leak (Tier-1 -> Non-Tier-1 -> Tier-1)",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					// Seed with some messages to pass the 120s/5msg threshold
					for i := 0; i < 5; i++ {
						p.classifyEvent("5.5.5.0/24", &MessageContext{
							Peer: "peer1", PathStr: "[3356 500 2914]", Now: now.Add(time.Duration(i*30) * time.Second),
						})
					}
				},
			},
			expectLevel2: Level2RouteLeak,
		},
		{
			name: "Path Hunting (Increasing length, some withdrawals)",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200]", PathLen: 2, Now: now})
					p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", IsWithdrawal: true, Now: now.Add(30 * time.Second)})
					// Need to set previous state to detect increases
					p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300]", PathLen: 3, Now: now.Add(60 * time.Second)})
					p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400]", PathLen: 4, Now: now.Add(90 * time.Second)})
					p.classifyEvent("6.6.6.0/24", &MessageContext{Peer: "p1", PathStr: "[100 200 300 400 500]", PathLen: 5, Now: now.Add(120 * time.Second)})
				},
			},
			expectLevel2: Level2PathHunting,
		},
		{
			name: "Policy Churn (Many community changes)",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					for i := 0; i < 6; i++ {
						p.classifyEvent("7.7.7.0/24", &MessageContext{
							Peer: "p1", PathStr: "[100]", CommStr: fmt.Sprintf("[[100:%d]]", i), Now: now.Add(time.Duration(i*25) * time.Second),
						})
					}
				},
			},
			expectLevel2: Level2PolicyChurn,
		},
		{
			name: "Path Length Oscillation",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					for i := 0; i < 4; i++ {
						p.classifyEvent("8.8.8.0/24", &MessageContext{
							Peer: "p1", PathStr: "[100 200]", PathLen: 2, Now: now.Add(time.Duration(i*60) * time.Second),
						})
						p.classifyEvent("8.8.8.0/24", &MessageContext{
							Peer: "p1", PathStr: "[100 200 300]", PathLen: 3, Now: now.Add(time.Duration(i*60+30) * time.Second),
						})
					}
				},
			},
			expectLevel2: Level2PathLengthOscillation,
		},
						{
							name: "Aggregator Flap",
							steps: []func(p *BGPProcessor, now time.Time){
								func(p *BGPProcessor, now time.Time){
									// Need > 10 aggregator changes AND rate > 0.05/s
									// We need to make sure changes are actually counted. 
									// Each PeerLastAttrs change counts.
									for i := 0; i < 20; i++ {
										peer := fmt.Sprintf("peer%d", i%5)
										p.classifyEvent("9.9.9.0/24", &MessageContext{
											Peer: peer, PathStr: "[100]", Aggregator: fmt.Sprintf("AS%d", i), Now: now.Add(time.Duration(i*5) * time.Second),
										})
									}
									// 20 messages in 100s. Rate = 0.2/s. totalAgg should be 20.
									// Final trigger after enough time
									p.classifyEvent("9.9.9.0/24", &MessageContext{
										Peer: "peer0", PathStr: "[100]", Aggregator: "AS-FINAL", Now: now.Add(130 * time.Second),
									})
								},
							},
							expectLevel2: Level2AggFlap,
						},		{
			name: "Next-Hop Flap",
			steps: []func(p *BGPProcessor, now time.Time){
				func(p *BGPProcessor, now time.Time) {
					// len(s.uniqueHops) > 1 && s.totalHop >= 5 && s.totalPath <= 1
					// Using 5 peers so total msgs = 10, perPeerRate = 2.0 (less than 5.0)
					for i := 0; i < 10; i++ {
						peer := fmt.Sprintf("peer%d", i%5)
						p.classifyEvent("10.10.10.0/24", &MessageContext{
							Peer: peer, PathStr: "[100]", NextHop: fmt.Sprintf("1.1.1.%d", i%2), Now: now.Add(time.Duration(i*10) * time.Second),
						})
					}
					// Add evaluation trigger after elapsed > 120s
					p.classifyEvent("10.10.10.0/24", &MessageContext{
						Peer: "peer0", PathStr: "[100]", NextHop: "1.1.1.0", Now: now.Add(130 * time.Second),
					})
				},
			},
			expectLevel2: Level2NextHopOscillation,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var lastLevel2 Level2EventType
			onEvent := func(lat, lng float64, cc string, eventType EventType, level2Type Level2EventType, prefix string, asn uint32) {
				if level2Type != Level2None {
					lastLevel2 = level2Type
				}
			}

			geo := func(ip uint32) (float64, float64, string) { return 0, 0, "US" }
			prefixToIP := func(p string) uint32 { return 0 }
			p := NewBGPProcessor(geo, nil, nil, nil, prefixToIP, onEvent)
			now := time.Now().Truncate(time.Hour) // Use stable time

			for _, step := range tt.steps {
				step(p, now)
			}

			if lastLevel2 != tt.expectLevel2 {
				t.Errorf("%s: expected %s, got %s", tt.name, tt.expectLevel2, lastLevel2)
			}
		})
	}
}
