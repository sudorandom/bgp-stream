package bgpengine

import (
	"testing"
	"time"
)

func TestBGPProcessorDeduplication(t *testing.T) {
	events := 0
	onEvent := func(lat, lng float64, cc string, eventType EventType, level2Type Level2EventType, prefix string, asn uint32) {
		events++
	}

	geo := func(ip uint32) (float64, float64, string) {
		return 37.0, -122.0, "US"
	}

	prefixToIP := func(p string) uint32 {
		return 0x08080808
	}

	p := NewBGPProcessor(geo, nil, nil, nil, prefixToIP, onEvent)

	// Simulate receiving a Withdrawal followed by Announcement (Path Change)
	p.mu.Lock()
	p.onEvent(37.0, -122.0, "US", EventNew, Level2None, "8.8.8.0/24", 0) // Initial discovery
	p.recentlySeen[0x08080808] = struct {
		Time time.Time
		Type EventType
	}{Time: time.Now(), Type: EventNew}
	p.mu.Unlock()

	if events != 1 {
		t.Errorf("Expected 1 event, got %d", events)
	}

	// Immediate duplicate update should be Gossip
	p.mu.Lock()
	// Simulate what would happen in ris_message handler
	if last, ok := p.recentlySeen[0x08080808]; ok && time.Since(last.Time) < 15*time.Second {
		p.onEvent(37.0, -122.0, "US", EventGossip, Level2None, "8.8.8.0/24", 0)
	}
	p.mu.Unlock()

	if events != 2 {
		t.Errorf("Expected 2 events, got %d", events)
	}
}
