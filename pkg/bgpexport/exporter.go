package bgpexport

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sudorandom/bgp-stream/pkg/bgp"
)

type Incident struct {
	ID        string                 `json:"id"`
	Prefix    string                 `json:"prefix"`
	ASN       uint32                 `json:"asn"`
	Type      bgp.ClassificationType `json:"type"`
	TypeName  string                 `json:"type_name"`
	StartTime time.Time              `json:"start_time"`
	EndTime   *time.Time             `json:"end_time,omitempty"`
	Details   interface{}            `json:"details,omitempty"`
}

type Exporter struct {
	dir             string
	activeIncidents map[string]*Incident // Key is "prefix" for now, or "prefix:type"
	mu              sync.Mutex
}

func NewExporter(dir string) *Exporter {
	return &Exporter{
		dir:             dir,
		activeIncidents: make(map[string]*Incident),
	}
}

func (e *Exporter) HandleEvent(prefix string, asn uint32, classType bgp.ClassificationType, leakDetail *bgp.LeakDetail, anomalyDetails *bgp.AnomalyDetails, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Consider bad states: Outage, Hijack, RouteLeak, Flap
	isBadState := classType == bgp.ClassificationOutage ||
		classType == bgp.ClassificationHijack ||
		classType == bgp.ClassificationRouteLeak ||
		classType == bgp.ClassificationFlap

	active, exists := e.activeIncidents[prefix]

	if isBadState {
		if exists {
			// If already in a bad state
			if active.Type == classType {
				// Same state, maybe update end time to now? Or wait until transition out
				return
			} else {
				// Transition to a NEW bad state
				e.endIncident(active, now)
				e.startIncident(prefix, asn, classType, leakDetail, anomalyDetails, now)
			}
		} else {
			// Start new incident
			e.startIncident(prefix, asn, classType, leakDetail, anomalyDetails, now)
		}
	} else {
		if exists {
			// Transition out of bad state
			e.endIncident(active, now)
			delete(e.activeIncidents, prefix)
		}
	}
}

func (e *Exporter) startIncident(prefix string, asn uint32, classType bgp.ClassificationType, leakDetail *bgp.LeakDetail, anomalyDetails *bgp.AnomalyDetails, now time.Time) {
	var details interface{}
	if leakDetail != nil {
		details = leakDetail
	} else if anomalyDetails != nil {
		details = anomalyDetails
	}

	incident := &Incident{
		ID:        uuid.New().String(),
		Prefix:    prefix,
		ASN:       asn,
		Type:      classType,
		TypeName:  classType.String(),
		StartTime: now,
		Details:   details,
	}
	e.activeIncidents[prefix] = incident
	e.writeIncident(incident)
}

func (e *Exporter) endIncident(incident *Incident, now time.Time) {
	incident.EndTime = &now
	e.writeIncident(incident)
}

func (e *Exporter) writeIncident(incident *Incident) {
	if err := os.MkdirAll(e.dir, 0755); err != nil {
		fmt.Printf("Error creating incident directory: %v\n", err)
		return
	}
	filePath := filepath.Join(e.dir, fmt.Sprintf("%s.json", incident.ID))
	data, err := json.MarshalIndent(incident, "", "  ")
	if err != nil {
		fmt.Printf("Error marshalling incident: %v\n", err)
		return
	}
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		fmt.Printf("Error writing incident file: %v\n", err)
	}
}
