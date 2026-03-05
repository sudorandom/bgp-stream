package bgpengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func runClassificationTest(t *testing.T, name string, expect ClassificationType, steps func(p *BGPProcessor, now time.Time, classify func(prefix string, ctx *MessageContext))) {
	t.Run(name, func(t *testing.T) {
		var lastClassification ClassificationType
		onEvent := func(lat, lng float64, cc, city string, eventType EventType, classificationType ClassificationType, prefix string, asn uint32, leakDetail ...*LeakDetail) {
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
					p.onEvent(lat, lng, cc, city, e.EventType, e.ClassificationType, e.Prefix, e.ASN, e.LeakDetail)
				}
			}
		}

		steps(p, now, classify)
		if lastClassification != expect {
			t.Errorf("%s: expected %s, got %s", name, expect, lastClassification)
		}
	})
}

func TestClassification(t *testing.T) {
	runClassificationTest(t, "Discovery (100 peers)", ClassificationDiscovery, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 100; i++ {
			classify("2.2.2.0/24", &MessageContext{
				Peer: fmt.Sprintf("peer%d", i), PathStr: "[100 200]", Now: now.Add(time.Duration(i) * time.Second),
			})
		}
	})

	runClassificationTest(t, "Discovery (Catch-all)", ClassificationDiscovery, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		for i := 0; i < 101; i++ {
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
		for i := 0; i < 3; i++ {
			classify("4.4.4.0/24", &MessageContext{
				Peer: "peer1", IsWithdrawal: true, Now: now.Add(time.Duration(i*50) * time.Second),
			})
		}
		classify("4.4.4.0/24", &MessageContext{
			Peer: "peer1", IsWithdrawal: true, Now: now.Add(150 * time.Second),
		})
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
}
