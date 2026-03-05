package bgpengine

import (
	"image/color"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/geoservice"
)

func TestUpdateHierarchicalRates(t *testing.T) {
	e := &Engine{
		VisualImpact: make(map[string]*VisualImpact),
	}

	prefix := "1.1.1.0/24"
	name := nameRouteLeak
	col := color.RGBA{255, 0, 0, 255}

	// First update with high-priority classification
	e.updateHierarchicalRates(prefix, name, "US", col, nil)

	vi, ok := e.VisualImpact[prefix]
	if !ok {
		t.Fatal("Expected VisualImpact to be created")
	}
	if vi.ClassificationName != name {
		t.Errorf("Expected ClassificationName %s, got %s", name, vi.ClassificationName)
	}
	if vi.ClassificationColor != col {
		t.Errorf("Expected ClassificationColor %v, got %v", col, vi.ClassificationColor)
	}

	// Second update with lower-priority classification (should not overwrite)
	e.updateHierarchicalRates(prefix, namePolicyChurn, "DE", color.RGBA{148, 0, 211, 255}, nil)
	if vi.ClassificationName != name {
		t.Errorf("Expected ClassificationName to remain %s, got %s", name, vi.ClassificationName)
	}

	// Third update with empty classification (should not overwrite)
	e.updateHierarchicalRates(prefix, "", "", color.RGBA{}, nil)
	if vi.ClassificationName != name {
		t.Errorf("Expected ClassificationName to remain %s, got %s", name, vi.ClassificationName)
	}

	// Fourth update with equally high priority (should overwrite or stay same)
	newName := nameHardOutage
	newCol := color.RGBA{255, 50, 50, 255}
	e.updateHierarchicalRates(prefix, newName, "JP", newCol, nil)
	if vi.ClassificationName != newName {
		t.Errorf("Expected ClassificationName to change to %s, got %s", newName, vi.ClassificationName)
	}
}

func TestGetPriority(t *testing.T) {
	e := &Engine{}
	tests := []struct {
		name     string
		priority int
	}{
		{nameRouteLeak, 3},
		{nameHardOutage, 3},
		{nameLinkFlap, 2},
		{namePolicyChurn, 1},
		{nameDiscovery, 0},
		{"", 0},
		{"Unknown", 0},
	}

	for _, tt := range tests {
		p := e.GetPriority(tt.name)
		if p != tt.priority {
			t.Errorf("Expected priority %d for %s, got %d", tt.priority, tt.name, p)
		}
	}
}

func TestEngineOutageClearing(t *testing.T) {
	e := NewEngine(1024, 768, 1.0)

	// Update mock to match new signature
	e.processor = NewBGPProcessor(func(ip uint32) (float64, float64, string, string, geoservice.ResolutionType) {
		return e.geo.GetIPCoords(ip)
	}, e.SeenDB, e.StateDB, e.asnMapping, e.RPKI, e.prefixToIP, e.Now, e.recordEvent)

	prefix := "1.2.3.0/24"

	// 1. Manually record an outage event
	e.recordEvent(0, 0, "US", "New York", EventUnknown, ClassificationOutage, prefix, 0)

	// Wait for event to process (since it's async now)
	time.Sleep(100 * time.Millisecond)

	if e.prefixToClassification[prefix] != ClassificationOutage {
		t.Errorf("Expected prefixToClassification to be ClassificationOutage, got %v", e.prefixToClassification[prefix])
	}
	if _, ok := e.currentAnomalies[ClassificationOutage][prefix]; !ok {
		t.Error("Expected prefix in currentAnomalies[ClassificationOutage]")
	}

	// 2. Record an announcement event (EventUpdate) for the same prefix
	// This should clear the ClassificationOutage from currentAnomalies
	e.recordEvent(0, 0, "US", "New York", EventUpdate, ClassificationNone, prefix, 0)

	// Wait for event to process
	time.Sleep(100 * time.Millisecond)

	if _, ok := e.currentAnomalies[ClassificationOutage][prefix]; ok {
		t.Error("Expected prefix to be cleared from currentAnomalies[ClassificationOutage]")
	}
}
