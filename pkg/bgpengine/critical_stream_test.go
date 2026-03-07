package bgpengine

import (
	"image/color"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func TestCriticalStreamDeduplication(t *testing.T) {
	e := &Engine{
		criticalCooldown: make(map[string]time.Time),
		asnMapping:       utils.NewASNMapping(),
	}
	// Initializing fonts and other UI stuff is not needed for this logic test

	c := color.RGBA{255, 0, 0, 255}
	name := nameHardOutage

	// Event 1: Outage for ASN 1234, prefix 1.1.0.0/16
	ev1 := &bgpEvent{
		classificationType: ClassificationOutage,
		prefix:             "1.1.0.0/16",
		asn:                1234,
		cc:                 "US",
	}
	e.recordToCriticalStream(ev1, c, name)

	if len(e.criticalQueue) != 1 {
		t.Fatalf("Expected 1 event in queue, got %d", len(e.criticalQueue))
	}

	// Wait 1s and update
	e.lastCriticalAddedAt = time.Now().Add(-2 * time.Second)
	e.updateCriticalStream()

	if len(e.CriticalStream) != 1 {
		t.Fatalf("Expected 1 event in stream, got %d", len(e.CriticalStream))
	}

	// Event 2: Same outage (same ASN), different prefix 1.2.0.0/16
	ev2 := &bgpEvent{
		classificationType: ClassificationOutage,
		prefix:             "1.2.0.0/16",
		asn:                1234,
		cc:                 "US",
	}
	e.recordToCriticalStream(ev2, c, name)

	if len(e.CriticalStream) != 1 {
		t.Errorf("Expected 1 event after deduplication, got %d", len(e.CriticalStream))
	}

	// Event 3: Outage with ASN 5678, prefix 2.2.0.0/16
	ev3 := &bgpEvent{
		classificationType: ClassificationOutage,
		prefix:             "2.2.0.0/16",
		asn:                5678,
		cc:                 "FR",
	}
	e.recordToCriticalStream(ev3, c, name)
	e.lastCriticalAddedAt = time.Now().Add(-2 * time.Second)
	e.updateCriticalStream()

	if len(e.CriticalStream) != 2 {
		t.Fatalf("Expected 2 events, got %d", len(e.CriticalStream))
	}

	// Event 4: Outage with ASN 0 (unknown) - should be ignored now
	ev4 := &bgpEvent{
		classificationType: ClassificationOutage,
		prefix:             "2.3.0.0/16",
		asn:                0,
		historicalASN:      5678,
		cc:                 "FR",
	}
	e.recordToCriticalStream(ev4, c, name)

	if len(e.CriticalStream) != 2 {
		t.Errorf("Expected 2 events (ASN 0 ignored), got %d", len(e.CriticalStream))
	}

	// Event 5: DDoS Mitigation, Provider 13335, Victim 9999, Prefix 3.3.0.0/16
	nameDDoS := nameDDoSMitigation
	ev5 := &bgpEvent{
		classificationType: ClassificationDDoSMitigation,
		prefix:             "3.3.0.0/16",
		asn:                13335,
		historicalASN:      9999,
		cc:                 "NL",
		leakDetail: &LeakDetail{
			LeakerASN: 13335,
			VictimASN: 9999,
		},
	}
	e.recordToCriticalStream(ev5, c, nameDDoS)
	e.lastCriticalAddedAt = time.Now().Add(-2 * time.Second)
	e.updateCriticalStream()

	if len(e.CriticalStream) != 3 {
		t.Fatalf("Expected 3 events, got %d", len(e.CriticalStream))
	}

	// Event 6: Same DDoS Mitigation, different prefix
	// This should now be deduplicated
	ev6 := &bgpEvent{
		classificationType: ClassificationDDoSMitigation,
		prefix:             "3.4.0.0/16",
		asn:                13335,
		historicalASN:      9999,
		cc:                 "NL",
		leakDetail: &LeakDetail{
			LeakerASN: 13335,
			VictimASN: 9999,
		},
	}
	e.recordToCriticalStream(ev6, c, nameDDoS)
	if len(e.CriticalStream) != 3 {
		t.Errorf("Expected 3 events after deduplication of DDoS mitigation, got %d", len(e.CriticalStream))
	}

	// Verify both prefixes are in the DDoS event
	var ddosEvent *CriticalEvent
	for _, ce := range e.CriticalStream {
		if ce.Anom == nameDDoSMitigation {
			ddosEvent = ce
			break
		}
	}
	if ddosEvent == nil {
		t.Fatal("DDoS event not found in stream")
	}
	if len(ddosEvent.ImpactedPrefixes) != 2 {
		t.Errorf("Expected 2 prefixes in DDoS event, got %d", len(ddosEvent.ImpactedPrefixes))
	}
	if _, ok := ddosEvent.ImpactedPrefixes["3.3.0.0/16"]; !ok {
		t.Error("Prefix 3.3.0.0/16 not found in DDoS event")
	}
	if _, ok := ddosEvent.ImpactedPrefixes["3.4.0.0/16"]; !ok {
		t.Error("Prefix 3.4.0.0/16 not found in DDoS event")
	}

	// Event 7: Cloudflare Self-Mitigation (Provider == Victim)
	// This was previously ignored, now should be allowed.
	ev7 := &bgpEvent{
		classificationType: ClassificationDDoSMitigation,
		prefix:             "1.3.0.0/16",
		asn:                13335,
		historicalASN:      13335,
		cc:                 "US",
		leakDetail: &LeakDetail{
			LeakerASN: 13335,
			VictimASN: 13335,
		},
	}
	e.recordToCriticalStream(ev7, c, nameDDoS)
	e.lastCriticalAddedAt = time.Now().Add(-2 * time.Second)
	e.updateCriticalStream()

	if len(e.CriticalStream) != 4 {
		t.Errorf("Expected 4 events (including Cloudflare self-mitigation), got %d", len(e.CriticalStream))
	}
}

type fakeWriteCloser struct{}

func (f *fakeWriteCloser) Write(p []byte) (n int, err error) { return len(p), nil }
func (f *fakeWriteCloser) Close() error                      { return nil }

func TestCriticalStreamExpiration(t *testing.T) {
	startTime := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	e := &Engine{
		criticalCooldown: make(map[string]time.Time),
		asnMapping:       utils.NewASNMapping(),
		virtualTime:      startTime,
		virtualStartTime: startTime, // Enable virtual time logic in e.Now()
	}

	// Mock Now() behavior without needing VideoWriter
	// Actually, looking at e.Now(), it needs VideoWriter != nil
	e.VideoWriter = &fakeWriteCloser{}

	c := color.RGBA{255, 0, 0, 255}
	name := nameHardOutage

	// T=0: Event 1 arrives
	ev1 := &bgpEvent{
		classificationType: ClassificationOutage,
		prefix:             "1.1.0.0/16",
		asn:                1234,
	}
	e.recordToCriticalStream(ev1, c, name)

	// Move from queue to stream (need to wait 1s in virtual time)
	e.virtualTime = startTime.Add(2 * time.Second)
	e.updateCriticalStream()

	if len(e.CriticalStream) != 1 {
		t.Fatalf("Expected 1 event in stream at T=2s, got %d", len(e.CriticalStream))
	}

	// T=5m: Duplicate event arrives
	e.virtualTime = startTime.Add(5 * time.Minute)
	e.recordToCriticalStream(ev1, c, name)

	// Move from queue to stream (need to wait 1s in virtual time)
	e.virtualTime = startTime.Add(2 * time.Second)
	e.updateCriticalStream()

	if len(e.CriticalStream) != 1 {
		t.Fatalf("Expected 1 event in stream at T=2s, got %d", len(e.CriticalStream))
	}

	// T=5m: Duplicate event arrives
	e.virtualTime = startTime.Add(5 * time.Minute)
	e.recordToCriticalStream(ev1, c, name)

	if len(e.CriticalStream) != 1 {
		t.Fatalf("Expected 1 event at T=5m, got %d", len(e.CriticalStream))
	}

	// T=11m: Event should NOT expire because there was an update at T=5m (11-5 = 6 < 10)
	e.virtualTime = startTime.Add(11 * time.Minute)
	e.updateMetrics() // This runs the cleanup logic

	if len(e.CriticalStream) != 1 {
		t.Errorf("Expected 1 event at T=11m (should not have expired), got %d", len(e.CriticalStream))
	}

	// T=16m: Event should now expire (16-5 = 11 > 10)
	e.virtualTime = startTime.Add(16 * time.Minute)
	e.updateMetrics()

	if len(e.CriticalStream) != 0 {
		t.Errorf("Expected 0 events at T=16m (expired), got %d", len(e.CriticalStream))
	}
}

func TestCriticalStreamTransition(t *testing.T) {
	e := &Engine{
		criticalCooldown: make(map[string]time.Time),
		asnMapping:       utils.NewASNMapping(),
	}
	c := color.RGBA{255, 0, 0, 255}
	name := nameHardOutage

	// 1. Add Outage for two prefixes
	ev1 := &bgpEvent{
		classificationType: ClassificationOutage,
		prefix:             "1.1.0.0/16", // 65536 IPs
		asn:                1234,
	}
	e.recordToCriticalStream(ev1, c, name)

	ev2 := &bgpEvent{
		classificationType: ClassificationOutage,
		prefix:             "1.2.0.0/16", // 65536 IPs
		asn:                1234,
	}
	e.recordToCriticalStream(ev2, c, name)

	e.lastCriticalAddedAt = time.Now().Add(-2 * time.Second)
	e.updateCriticalStream()

	if len(e.CriticalStream) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(e.CriticalStream))
	}
	ce := e.CriticalStream[0]
	expectedIPs := uint64(65536 * 2)
	if ce.ImpactedIPs != expectedIPs {
		t.Errorf("Expected %d IPs, got %d", expectedIPs, ce.ImpactedIPs)
	}

	// 2. Transition one prefix to Discovery (Not critical)
	ev1Recovery := &bgpEvent{
		classificationType: ClassificationDiscovery,
		prefix:             "1.1.0.0/16",
		asn:                1234,
	}
	// The name passed here should match the existing event's name for it to be found
	e.recordToCriticalStream(ev1Recovery, color.RGBA{}, name)

	if ce.ImpactedIPs != 65536 {
		t.Errorf("Expected impacted IPs to drop to 65536, got %d", ce.ImpactedIPs)
	}
	if len(ce.ImpactedPrefixes) != 1 {
		t.Errorf("Expected 1 prefix remaining, got %d", len(ce.ImpactedPrefixes))
	}

	// 3. Transition the other prefix to Discovery
	ev2Recovery := &bgpEvent{
		classificationType: ClassificationDiscovery,
		prefix:             "1.2.0.0/16",
		asn:                1234,
	}
	e.recordToCriticalStream(ev2Recovery, color.RGBA{}, name)

	if len(e.CriticalStream) != 0 {
		t.Errorf("Expected event to be removed from stream after all prefixes recovered, but still have %d events", len(e.CriticalStream))
	}
}
