package bgpengine

import (
	"fmt"
	"testing"
	"time"
)

const testPrefix = "1.2.3.0/24"

func TestAggregatorFlap_MultiHost(t *testing.T) {
	// This test verifies that different aggregator values from different hosts
	// for the same peer IP do NOT trigger an aggregator flap.

	runClassificationTest(t, "Aggregator Flap (Multi-Host False Positive)", ClassificationNone, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		peerIP := "10.0.0.1"

		for i := 0; i < 20; i++ {
			host := "rrc00"
			agg := "65001:1.1.1.1"
			if i%2 == 1 {
				host = "rrc01"
				agg = "65002:2.2.2.2"
			}

			classify(testPrefix, &MessageContext{
				Peer:       peerIP,
				Host:       host,
				Aggregator: agg,
				PathStr:    "[100 200]",
				Now:        now.Add(time.Duration(i) * time.Second),
			})
		}
	})
}

func TestAggregatorFlap_SingleHost(t *testing.T) {
	// This test verifies that different aggregator values from the SAME host
	// and same peer IP DO trigger an aggregator flap.

	runClassificationTest(t, "Aggregator Flap (Single-Host Real Flap)", ClassificationAggFlap, func(p *BGPProcessor, now time.Time, classify func(string, *MessageContext)) {
		peerIP := "10.0.0.1"
		host := "rrc00"

		for i := 0; i < 20; i++ {
			agg := fmt.Sprintf("65001:%d.%d.%d.%d", i, i, i, i)
			classify(testPrefix, &MessageContext{
				Peer:       peerIP,
				Host:       host,
				Aggregator: agg,
				PathStr:    "[100 200]",
				Now:        now.Add(time.Duration(i) * time.Second),
			})
		}
	})
}
