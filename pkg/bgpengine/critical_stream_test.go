package bgpengine

import (
	"image/color"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/bgp"
)

func TestGenerateInsights(t *testing.T) {
	e := &Engine{
		lastInsights: make(map[string]time.Time),
	}

	state := &statsWorkerState{
		asnSortedGroups: []*asnGroup{
			{
				asnStr:     "AS1234",
				anom:       bgp.NameHardOutage,
				totalCount: 50,
			},
			{
				asnStr:     "AS5678",
				anom:       bgp.NameDDoSMitigation,
				totalCount: 1500,
			},
		},
	}

	prefixCounts := []PrefixCount{
		{
			Type:     bgp.ClassificationOutage,
			Count:    10,
			IPCount:  50000,
			ASNCount: 2,
			Color:    color.RGBA{255, 0, 0, 255},
		},
		{
			Type:     bgp.ClassificationDDoSMitigation,
			Count:    5,
			IPCount:  1000,
			ASNCount: 1,
			Color:    color.RGBA{218, 112, 214, 255},
		},
	}

	// Fake churn rate > 500
	e.metricsMu.Lock()
	e.rateUpd = 400
	e.rateWith = 200
	e.metricsMu.Unlock()

	e.generateInsights(state, prefixCounts)

	if len(e.InsightStream) != 3 {
		t.Fatalf("Expected 3 insights (Outage, DDoS, Churn), got %d", len(e.InsightStream))
	}

	// Validate Outage Insight
	foundOutage := false
	for _, ie := range e.InsightStream {
		if ie.Category == "Outage" {
			foundOutage = true
			if len(ie.Lines) != 3 { // Impacted, Networks, Worst ASN
				t.Errorf("Expected 3 lines for Outage, got %d", len(ie.Lines))
			}
			if ie.Title != "MAJOR OUTAGE DETECTED" {
				t.Errorf("Unexpected title for Outage: %s", ie.Title)
			}
		}
	}
	if !foundOutage {
		t.Errorf("Outage insight not found")
	}

	// Test cooldowns: run again immediately, should not emit new insights
	e.generateInsights(state, prefixCounts)
	if len(e.InsightStream) != 3 {
		t.Fatalf("Expected insights to be throttled, still got %d", len(e.InsightStream))
	}
}
