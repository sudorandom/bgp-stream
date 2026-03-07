package bgpengine

import (
	"testing"

	"github.com/sudorandom/bgp-stream/pkg/bgp"
)

func TestGetPriority(t *testing.T) {
	e := &Engine{}
	tests := []struct {
		name     string
		priority int
	}{
		{bgp.NameRouteLeak, 3},
		{bgp.NameHardOutage, 3},
		{bgp.NameHijack, 3},
		{bgp.NameDDoSMitigation, 1},
		{bgp.NameFlap, 2},
		{bgp.NameTrafficEng, 1},
		{bgp.NameDiscovery, 0},
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
