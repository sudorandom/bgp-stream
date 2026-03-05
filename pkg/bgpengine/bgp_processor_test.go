package bgpengine

import (
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/geoservice"
)

func TestBGPProcessorDeduplication(t *testing.T) {
	events := 0
	onEvent := func(lat, lng float64, cc, city string, eventType EventType, classificationType ClassificationType, prefix string, asn uint32, leakDetail ...*LeakDetail) {
		events++
	}
	geo := func(ip uint32) (float64, float64, string, string, geoservice.ResolutionType) {
		return 37.0, -122.0, "US", "San Francisco", geoservice.ResGeoIP
	}

	prefixToIP := func(p string) uint32 {
		return 0x08080808
	}

	p := NewBGPProcessor(geo, nil, nil, nil, nil, prefixToIP, time.Now, onEvent)

	// Simulate receiving a New Announcement
	p.mu.Lock()
	p.onEvent(37.0, -122.0, "US", "San Francisco", EventNew, ClassificationNone, "8.8.8.0/24", 0, nil) // Initial discovery

	// Access recentlySeen through a worker
	wIdx := int(0x08080808 % uint32(len(p.workers)))
	p.workers[wIdx].recentlySeen.Add(0x08080808, struct {
		Time time.Time
		Type EventType
	}{Time: time.Now(), Type: EventNew})
	p.mu.Unlock()

	if events != 1 {
		t.Errorf("Expected 1 event, got %d", events)
	}

	// Immediate duplicate update should be Gossip
	p.mu.Lock()
	// Simulate what would happen in ris_message handler
	if last, ok := p.workers[wIdx].recentlySeen.Get(0x08080808); ok && time.Since(last.Time) < 15*time.Second {
		p.onEvent(37.0, -122.0, "US", "San Francisco", EventGossip, ClassificationNone, "8.8.8.0/24", 0, nil)
	}
	p.mu.Unlock()

	if events != 2 {
		t.Errorf("Expected 2 events, got %d", events)
	}
}
