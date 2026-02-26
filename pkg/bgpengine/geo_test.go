package bgpengine

import (
	"math"
	"testing"
)

func TestProject(t *testing.T) {
	g := NewGeoService(1920, 1080, 380.0)

	tests := []struct {
		lat, lng float64
		wantX, wantY float64
	}{
		{0, 0, 960, 540},
		{90, 0, 960, 3.14},      // Near North Pole
		{-90, 0, 960, 1076.86},  // Near South Pole
		{0, 180, 2034.72, 540},  // Far East
		{0, -180, -114.72, 540}, // Far West
	}

	for _, tt := range tests {
		x, y := g.Project(tt.lat, tt.lng)
		if math.Abs(x-tt.wantX) > 1.0 || math.Abs(y-tt.wantY) > 1.0 {
			t.Errorf("Project(%f, %f) = (%f, %f); want (%f, %f)", tt.lat, tt.lng, x, y, tt.wantX, tt.wantY)
		}
	}
}

func TestGetIPCoords(t *testing.T) {
	g := NewGeoService(1920, 1080, 380.0)
	
	// Mock some data
	g.prefixData = PrefixData{
		L: []Location{
			{37.7749, -122.4194, "US", "San Francisco"},
		},
		R: []uint32{
			0x08080800, 0, // 8.8.8.0/24 -> SF
		},
	}

	lat, lng, cc := g.GetIPCoords(0x08080808) // 8.8.8.8
	if lat == 0 || lng == 0 || cc != "US" {
		t.Errorf("GetIPCoords(8.8.8.8) = (%f, %f, %s); want SF coordinates and US", lat, lng, cc)
	}
	
	if lat != 37.7749 || lng != -122.4194 {
		t.Errorf("Expected SF coordinates (37.7749, -122.4194), got (%f, %f)", lat, lng)
	}
}
