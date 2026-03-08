package bgpexport

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sudorandom/bgp-stream/pkg/bgp"
)

func TestExporter(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "bgpexport-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	exporter := NewExporter(tempDir)
	now := time.Now()

	prefix := "192.168.1.0/24"
	asn := uint32(12345)

	// Start an incident
	exporter.HandleEvent(prefix, asn, bgp.ClassificationOutage, nil, now)

	// Read the file
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d", len(files))
	}

	fileContent, err := os.ReadFile(filepath.Join(tempDir, files[0].Name()))
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	var incident Incident
	err = json.Unmarshal(fileContent, &incident)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if incident.Prefix != prefix {
		t.Errorf("Expected prefix %s, got %s", prefix, incident.Prefix)
	}

	if incident.Type != bgp.ClassificationOutage {
		t.Errorf("Expected type %v, got %v", bgp.ClassificationOutage, incident.Type)
	}

	if incident.EndTime != nil {
		t.Errorf("Expected EndTime to be nil, got %v", incident.EndTime)
	}

	// End the incident
	now2 := now.Add(time.Minute)
	exporter.HandleEvent(prefix, asn, bgp.ClassificationNone, nil, now2)

	// Read the file again
	fileContent, err = os.ReadFile(filepath.Join(tempDir, files[0].Name()))
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	err = json.Unmarshal(fileContent, &incident)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if incident.EndTime == nil {
		t.Errorf("Expected EndTime to be non-nil")
	} else if !incident.EndTime.Equal(now2) {
		t.Errorf("Expected EndTime to be %v, got %v", now2, incident.EndTime)
	}
}
