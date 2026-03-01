package bgpengine

import (
	"image/color"
	"testing"
)

func TestUpdateHierarchicalRates(t *testing.T) {
	e := &Engine{
		VisualImpact: make(map[string]*VisualImpact),
	}

	prefix := "1.1.1.0/24"
	name := nameRouteLeak
	col := color.RGBA{255, 0, 0, 255}

	// First update with high-priority classification
	e.updateHierarchicalRates(prefix, name, col)

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
	e.updateHierarchicalRates(prefix, namePolicyChurn, color.RGBA{148, 0, 211, 255})
	if vi.ClassificationName != name {
		t.Errorf("Expected ClassificationName to remain %s, got %s", name, vi.ClassificationName)
	}

	// Third update with empty classification (should not overwrite)
	e.updateHierarchicalRates(prefix, "", color.RGBA{})
	if vi.ClassificationName != name {
		t.Errorf("Expected ClassificationName to remain %s, got %s", name, vi.ClassificationName)
	}

	// Fourth update with equally high priority (should overwrite or stay same)
	newName := nameHardOutage
	newCol := color.RGBA{255, 50, 50, 255}
	e.updateHierarchicalRates(prefix, newName, newCol)
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
		{nameBabbling, 2},
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
