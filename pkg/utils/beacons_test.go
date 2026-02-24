package utils

import "testing"

func TestIsBeaconPrefix(t *testing.T) {
	tests := []struct {
		prefix   string
		isBeacon bool
	}{
		{"84.205.65.0/24", true},
		{"84.205.81.0/24", true},
		{"8.8.8.8/32", false},
		{"1.1.1.1/32", false},
		{"84.205.82.0/24", true},
		{"93.175.154.0/25", true},
		{"93.175.154.128/28", true},
		{"93.175.146.0/24", true},
		{"93.175.147.0/24", true},
	}

	for _, tt := range tests {
		if got := IsBeaconPrefix(tt.prefix); got != tt.isBeacon {
			t.Errorf("IsBeaconPrefix(%q) = %v, want %v", tt.prefix, got, tt.isBeacon)
		}
	}
}
