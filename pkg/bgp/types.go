// Package bgp provides core logic for BGP stream processing, classification, and analysis.
package bgp

import (
	"time"
)

type EventType int

const (
	EventUnknown EventType = iota
	EventNew
	EventUpdate
	EventWithdrawal
	EventGossip
)

func (t EventType) String() string {
	switch t {
	case EventNew:
		return "new"
	case EventUpdate:
		return "upd"
	case EventWithdrawal:
		return "with"
	case EventGossip:
		return "gossip"
	default:
		return "unknown"
	}
}

type LeakType int

const (
	LeakUnknown LeakType = iota
	LeakHairpin
	LeakLateral
	LeakProviderToPeer
	LeakPeerToProvider
	LeakReOrigination
	DDoSRTBH
	DDoSFlowspec
	DDoSTrafficRedirection
)

const (
	StrUnknown = "Unknown"
)

func (t LeakType) String() string {
	switch t {
	case LeakHairpin:
		return "Hairpin Turn"
	case LeakLateral:
		return "Lateral Infection"
	case LeakProviderToPeer:
		return "Provider to Peer"
	case LeakPeerToProvider:
		return "Peer to Provider"
	case LeakReOrigination:
		return "Prefix Re-Origination"
	case DDoSRTBH:
		return "RTBH"
	case DDoSFlowspec:
		return "Flowspec"
	case DDoSTrafficRedirection:
		return "Traffic Redirection"
	default:
		return StrUnknown
	}
}

type LeakDetail struct {
	Type      LeakType
	LeakerASN uint32
	VictimASN uint32
}

type ClassificationType int

const (
	NameFlap           = "Flap"
	NamePathHunting    = "Path Hunting"
	NameTrafficEng     = "Traffic Eng."
	NameHardOutage     = "Outage"
	NameRouteLeak      = "Route Leak"
	NameDiscovery      = "Discovery"
	NameDDoSMitigation = "DDoS Mitigation"
	NameHijack         = "BGP Hijack"
	NameBogon          = "Bogon/Martian"
)

const (
	ClassificationNone ClassificationType = iota
	ClassificationFlap
	ClassificationPathHunting
	ClassificationTrafficEngineering
	ClassificationOutage
	ClassificationRouteLeak
	ClassificationDiscovery
	ClassificationDDoSMitigation
	ClassificationHijack
	ClassificationBogon
)

func (t ClassificationType) String() string {
	switch t {
	case ClassificationFlap:
		return NameFlap
	case ClassificationPathHunting:
		return NamePathHunting
	case ClassificationTrafficEngineering:
		return NameTrafficEng
	case ClassificationOutage:
		return NameHardOutage
	case ClassificationRouteLeak:
		return NameRouteLeak
	case ClassificationDiscovery:
		return NameDiscovery
	case ClassificationDDoSMitigation:
		return NameDDoSMitigation
	case ClassificationHijack:
		return NameHijack
	case ClassificationBogon:
		return NameBogon
	default:
		return "None"
	}
}

type MessageContext struct {
	IsWithdrawal   bool
	NumPrefixes    int
	PathStr        string
	CommStr        string
	NextHop        string
	Aggregator     string
	PathLen        int
	Peer           string
	Host           string
	OriginASN      uint32
	LastRpkiStatus int32
	LastOriginAsn  uint32
	Med            int32
	LocalPref      int32
	Now            time.Time
}

func (ctx *MessageContext) EventType() EventType {
	if ctx.IsWithdrawal {
		return EventWithdrawal
	}
	return EventUpdate
}
