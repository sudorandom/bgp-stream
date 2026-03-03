package bgpengine

import (
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgpengine/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type Level2EventType int

const (
	nameLinkFlap        = "Link Flap"
	nameAggFlap         = "Aggregator Flap"
	namePathOscillation = "Path Oscillation"
	nameBabbling        = "Babbling"
	namePathHunting     = "Path Hunting"
	namePolicyChurn     = "Policy Churn"
	nameNextHopFlap     = "Next-Hop Flap"
	nameHardOutage      = "Outage"
	nameRouteLeak       = "Route Leak"
	nameDiscovery       = "Discovery"
)

const (
	Level2None Level2EventType = iota
	Level2LinkFlap
	Level2AggFlap
	Level2PathLengthOscillation
	Level2Babbling
	Level2PathHunting
	Level2PolicyChurn
	Level2NextHopOscillation
	Level2Outage
	Level2RouteLeak
	Level2Discovery
)

func (t Level2EventType) String() string {
	switch t {
	case Level2LinkFlap:
		return nameLinkFlap
	case Level2AggFlap:
		return nameAggFlap
	case Level2PathLengthOscillation:
		return namePathOscillation
	case Level2Babbling:
		return nameBabbling
	case Level2PathHunting:
		return namePathHunting
	case Level2PolicyChurn:
		return namePolicyChurn
	case Level2NextHopOscillation:
		return nameNextHopFlap
	case Level2Outage:
		return nameHardOutage
	case Level2RouteLeak:
		return nameRouteLeak
	case Level2Discovery:
		return nameDiscovery
	default:
		return "None"
	}
}

type prefixStats struct {
	totalAnn, totalWith, totalMsgs           int32
	totalPath, totalComm, totalHop, totalAgg int32
	totalIncreases, totalDecreases           int32
	totalMed, totalLP                        int32
	earliestTS                               int64
	uniqueHops                               map[string]bool
	uniqueASNs                               map[uint32]bool
	uniquePeers                              map[string]bool
	uniqueHosts                              map[string]bool
}

type BGPClassifier struct {
	seenDB     *utils.DiskTrie
	asnMapping *utils.ASNMapping

	mu                   sync.Mutex
	level2Stats          map[Level2EventType]int
	level2UniquePrefixes map[Level2EventType]map[string]struct{}
	totalLevel2Events    int
}

func NewBGPClassifier(seenDB *utils.DiskTrie, asnMapping *utils.ASNMapping) *BGPClassifier {
	return &BGPClassifier{
		seenDB:               seenDB,
		asnMapping:           asnMapping,
		level2Stats:          make(map[Level2EventType]int),
		level2UniquePrefixes: make(map[Level2EventType]map[string]struct{}),
	}
}

func (c *BGPClassifier) Evaluate(prefix string, state *bgpproto.PrefixState, ctx *MessageContext) (Level2EventType, uint32, bool) {
	stats := c.aggregateRecentBuckets(state, ctx.Now, ctx.OriginASN)

	elapsed := float64(ctx.Now.Unix() - stats.earliestTS)
	if elapsed < 60 {
		elapsed = float64(ctx.Now.Unix() - state.StartTimeTs)
	}
	if elapsed <= 0 {
		elapsed = 1
	}

	// Ensure we have seen enough messages over a small time window to classify
	if elapsed < 120 && stats.totalMsgs < 5 {
		return Level2None, 0, false
	}

	eventType, classified := c.findClassification(prefix, state, &stats, elapsed, ctx)

	if classified {
		if state.ClassifiedType != 0 {
			if c.getPriority(eventType) <= c.getPriority(Level2EventType(state.ClassifiedType)) {
				// We already have a classification of equal or higher priority
				// Just return the event for the current classification
				return Level2EventType(state.ClassifiedType), ctx.OriginASN, true
			}
		}

		originASN := uint32(0)
		for _, attr := range state.PeerLastAttrs {
			if attr.OriginAsn != 0 {
				originASN = attr.OriginAsn
				break
			}
		}

		c.recordClassification(prefix, state, eventType, ctx.Now.Unix())
		return eventType, originASN, true
	}
	return Level2None, 0, false
}

func (c *BGPClassifier) aggregateRecentBuckets(state *bgpproto.PrefixState, now time.Time, currentOriginASN uint32) prefixStats {
	for peer, attr := range state.PeerLastAttrs {
		if now.Unix()-attr.LastUpdateTs > 3600 {
			delete(state.PeerLastAttrs, peer)
		}
	}

	cutoff := now.Add(-10 * time.Minute).Unix()
	s := prefixStats{
		earliestTS:  now.Unix(),
		uniqueHops:  make(map[string]bool),
		uniqueASNs:  make(map[uint32]bool),
		uniquePeers: make(map[string]bool),
		uniqueHosts: make(map[string]bool),
	}

	for ts, b := range state.Buckets {
		if ts < cutoff {
			continue
		}
		if ts < s.earliestTS {
			s.earliestTS = ts
		}
		s.totalAnn += b.Announcements
		s.totalWith += b.Withdrawals
		s.totalMsgs += b.TotalMessages
		s.totalPath += b.PathChanges
		s.totalComm += b.CommunityChanges
		s.totalHop += b.NextHopChanges
		s.totalAgg += b.AggregatorChanges
		s.totalIncreases += b.PathLengthIncreases
		s.totalDecreases += b.PathLengthDecreases
		s.totalMed += b.MedChanges
		s.totalLP += b.LocalPrefChanges
	}

	for peer, attr := range state.PeerLastAttrs {
		if attr.NextHop != "" {
			s.uniqueHops[attr.NextHop] = true
		}
		if attr.OriginAsn != 0 {
			s.uniqueASNs[attr.OriginAsn] = true
		}
		// Only count peers and hosts that are currently seeing the same origin ASN
		if attr.OriginAsn == currentOriginASN {
			s.uniquePeers[peer] = true
			if attr.Host != "" {
				s.uniqueHosts[attr.Host] = true
			}
		}
	}
	return s
}

func (c *BGPClassifier) findClassification(prefix string, state *bgpproto.PrefixState, s *prefixStats, elapsed float64, ctx *MessageContext) (Level2EventType, bool) {
	numPeers := float64(len(state.PeerLastAttrs))
	if numPeers == 0 {
		numPeers = 1
	}
	perPeerRate := float64(s.totalMsgs) / numPeers

	// 1. Critical
	if et, ok := c.findCriticalAnomaly(prefix, s, elapsed, ctx); ok {
		return et, true
	}

	// 2. Bad
	if et, ok := c.findBadAnomaly(s, elapsed, perPeerRate); ok {
		return et, true
	}

	// 3. Normal / Policy
	if et, ok := c.findNormalAnomaly(s, elapsed); ok {
		return et, true
	}

	return Level2None, false
}

func (c *BGPClassifier) getHistoricalASN(prefix string) uint32 {
	if c.seenDB == nil {
		return 0
	}
	val, _ := c.seenDB.Get(prefix)
	if len(val) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(val)
}

func (c *BGPClassifier) findCriticalAnomaly(prefix string, s *prefixStats, elapsed float64, ctx *MessageContext) (Level2EventType, bool) {
	if s.totalWith >= 3 && s.totalAnn == 0 && elapsed > 60 {
		return Level2Outage, true
	}

	peerCount := len(s.uniquePeers)
	hostCount := len(s.uniqueHosts)

	// 1. RPKI Validation Check (The Signal)
	if ctx.OriginASN != 0 && utils.RPKIStatus(ctx.LastRpkiStatus) == utils.RPKIInvalidASN {
		isDDoS := c.isDDoSProvider(ctx.OriginASN)

		// Historical Origin Check (The Filter)
		historicalASN := c.getHistoricalASN(prefix)
		isTransition := historicalASN != 0 && historicalASN != ctx.OriginASN

		if !isDDoS {
			// HIGH SIGNAL HIJACK:
			// - RPKI Invalid: ASN
			// - Origin has changed from what we saw historically
			// - Seen across 3+ peers and 2+ RIPE collectors
			if isTransition && peerCount >= 3 && hostCount >= 2 {
				log.Printf("[!!! HIJACK TRANSITION !!!] Prefix: %s, New Origin: AS%d, Prev Origin: AS%d, RPKI: InvalidASN, Consensus: %d peers/%d hosts",
					prefix, ctx.OriginASN, historicalASN, peerCount, hostCount)
				return Level2RouteLeak, true
			}

			// MEDIUM SIGNAL: New prefix (never seen before) that is RPKI invalid
			// We require more evidence for these as they are often just first-time seen misconfigs
			if historicalASN == 0 && peerCount >= 5 && hostCount >= 3 {
				log.Printf("[!!! HIJACK NEW PREFIX !!!] Prefix: %s, Origin: AS%d, RPKI: InvalidASN, Consensus: %d peers/%d hosts",
					prefix, ctx.OriginASN, peerCount, hostCount)
				return Level2RouteLeak, true
			}
		}
	}

	// 2. AS Path Valley-Free Check (The Heuristic)
	if segment, ok := c.hasRouteLeak(ctx); ok {
		// Consensus requirement for path violations to filter out terminal edge/collector leaks.
		// If we see a Tier-1 -> Stub -> Tier-1 path from multiple vantage points,
		// it is highly likely to be a real traffic-impacting leak.
		if peerCount >= 3 && hostCount >= 2 {
			c.logRouteLeak(prefix, segment)
			return Level2RouteLeak, true
		}
	}

	return Level2None, false
}

func (c *BGPClassifier) isDDoSProvider(asn uint32) bool {
	// Known Scrubbing ASNs, major clouds, and large tech networks that often trigger RPKI false positives
	scrubbers := map[uint32]bool{
		13335:  true, // Cloudflare
		20940:  true, // Akamai
		16509:  true, // Amazon
		14618:  true, // Amazon
		15169:  true, // Google
		8075:   true, // Microsoft
		32934:  true, // Facebook
		19324:  true, // Akamai/Prolexic
		6428:   true, // Radware
		19551:  true, // Incapsula
		31898:  true, // Oracle
		40027:  true, // Oracle
		36040:  true, // Google Cloud
		109:    true, // Cisco
		714:    true, // Apple
		22822:  true, // LinkedIn
		13238:  true, // Yandex
		2906:   true, // Netflix
		6939:   true, // Hurricane Electric (Large Backbone)
		174:    true, // Cogent (Large Backbone)
		2914:   true, // NTT (Large Backbone)
		3356:   true, // Level 3 (Large Backbone)
		6762:   true, // Telecom Italia Sparkle (Large Backbone)
		1299:   true, // Telia (Large Backbone)
		6453:   true, // Tata (Large Backbone)
		1239:   true, // Sprint (Large Backbone)
		701:    true, // Verizon (Large Backbone)
		7018:   true, // AT&T (Large Backbone)
		1273:   true, // Vodafone (Large Backbone)
		4637:   true, // Telstra (Large Backbone)
		197730: true, // Sea-Bone (Large Backbone)
	}
	return scrubbers[asn]
}

func (c *BGPClassifier) hasRouteLeak(ctx *MessageContext) ([3]uint32, bool) {
	if ctx.PathStr == "" {
		return [3]uint32{}, false
	}
	// Basic path parser (path is stored as string "[1 2 3]")
	fields := strings.Fields(strings.Trim(ctx.PathStr, "[]"))
	if len(fields) < 3 {
		return [3]uint32{}, false
	}

	rawPath := make([]uint32, 0, len(fields))
	for _, f := range fields {
		var asn uint32
		if _, err := fmt.Sscanf(f, "%d", &asn); err == nil {
			rawPath = append(rawPath, asn)
		}
	}

	// 1. Collapse Sibling ASNs (Same Organization)
	// Many large providers (like Telstra AS1221 and AS4637) use multiple ASNs for internal routing.
	// Transitioning between sibling ASNs is not a route leak.
	if len(rawPath) < 2 {
		return [3]uint32{}, false
	}

	path := make([]uint32, 0, len(rawPath))
	path = append(path, rawPath[0])
	lastOrgID := ""
	if c.asnMapping != nil {
		lastOrgID = c.asnMapping.GetOrgID(rawPath[0])
	}

	for i := 1; i < len(rawPath); i++ {
		currentASN := rawPath[i]
		currentOrgID := ""
		if c.asnMapping != nil {
			currentOrgID = c.asnMapping.GetOrgID(currentASN)
		}

		// Only add if it's a different ASN AND not a sibling ASN
		if currentASN != rawPath[i-1] {
			if currentOrgID == "" || currentOrgID != lastOrgID {
				path = append(path, currentASN)
				lastOrgID = currentOrgID
			}
		}
	}

	if len(path) < 3 {
		return [3]uint32{}, false
	}

	// 2. Valley-free violation check on collapsed path: Tier-1 -> Non-Tier-1/Non-Cloud -> Tier-1
	for i := 0; i < len(path)-2; i++ {
		if c.isTier1(path[i]) && !c.isTier1(path[i+1]) && !c.isCloud(path[i+1]) && c.isTier1(path[i+2]) {
			return [3]uint32{path[i], path[i+1], path[i+2]}, true
		}
	}

	return [3]uint32{}, false
}

func (c *BGPClassifier) logRouteLeak(prefix string, segment [3]uint32) {
	pathStrs := make([]string, 3)
	for i, asn := range segment {
		name := "Unknown"
		if c.asnMapping != nil {
			name = c.asnMapping.GetName(asn)
		}
		pathStrs[i] = fmt.Sprintf("AS%d (%s)", asn, name)
	}
	log.Printf("[!!! ROUTE LEAK !!!] Prefix: %s, Path Segment: %s", prefix, strings.Join(pathStrs, " -> "))
}

func (c *BGPClassifier) isTier1(asn uint32) bool {
	switch asn {
	case 209, 701, 702, 1239, 1299, 2828, 2914, 3257, 3320, 3356, 3491, 3549, 3561, 5511, 6453, 6461, 6762, 6830, 7018, 12956: // Global Tier-1s
		return true
	case 174, 6939, 9002, 1273, 4637, 7922, 4134, 4809, 4837, 7473, 9808: // Major Regional/National Backbones
		return true
	default:
		return false
	}
}

func (c *BGPClassifier) isCloud(asn uint32) bool {
	switch asn {
	case 13335, 15169, 16509, 14618, 20940, 8075, 32934, 31898, 40027, 36040, 54113, 14061, 37963, 45102, 16625: // Major Cloud/CDN
		return true
	default:
		return false
	}
}

func (c *BGPClassifier) findBadAnomaly(s *prefixStats, elapsed, perPeerRate float64) (Level2EventType, bool) {
	isAggFlap := s.totalAgg >= 10 && float64(s.totalAgg)/elapsed > 0.05
	isNextHopOsc := len(s.uniqueHops) > 1 && s.totalHop >= 10 && s.totalPath <= 2
	isLinkFlap := s.totalWith >= 5 && float64(s.totalAnn)/float64(s.totalWith) < 2.0

	if isAggFlap {
		return Level2AggFlap, true
	}
	if isNextHopOsc {
		return Level2NextHopOscillation, true
	}
	if isLinkFlap {
		return Level2LinkFlap, true
	}

	// Babbling is the catch-all for "Bad" high volume activity
	if perPeerRate >= 2.0 && s.totalMsgs >= 20 && s.totalPath == 0 && s.totalComm == 0 && s.totalMed == 0 && s.totalLP == 0 && s.totalAgg == 0 && s.totalHop == 0 {
		return Level2Babbling, true
	}
	return Level2None, false
}

func (c *BGPClassifier) findNormalAnomaly(s *prefixStats, elapsed float64) (Level2EventType, bool) {
	isPathHunting := s.totalAnn >= 5 && s.totalIncreases >= 2 && s.totalWith >= 1
	isPolicyChurn := s.totalComm >= 10 || (s.totalPath >= 10 && s.totalIncreases+s.totalDecreases <= 2) || (s.totalMed+s.totalLP >= 5 && s.totalPath <= 5)
	isPathLengthOsc := (s.totalIncreases+s.totalDecreases) >= 5 && float64(s.totalIncreases+s.totalDecreases)/elapsed > 0.01

	if isPathHunting {
		return Level2PathHunting, true
	}
	if isPolicyChurn {
		return Level2PolicyChurn, true
	}
	if isPathLengthOsc {
		return Level2PathLengthOscillation, true
	}

	// Discovery as the catch-all for high volume activity (>= 100 messages)
	// that didn't match any "Bad" anomaly or specific "Normal" pattern.
	if s.totalMsgs >= 100 {
		return Level2Discovery, true
	}
	return Level2None, false
}

func (c *BGPClassifier) recordClassification(prefix string, state *bgpproto.PrefixState, eventType Level2EventType, now int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.level2Stats[eventType]++
	if c.level2UniquePrefixes[eventType] == nil {
		c.level2UniquePrefixes[eventType] = make(map[string]struct{})
	}
	c.level2UniquePrefixes[eventType][prefix] = struct{}{}
	c.totalLevel2Events++

	// Record that this prefix is now classified
	state.ClassifiedType = int32(eventType)
	state.ClassifiedTimeTs = now
}

func (c *BGPClassifier) getPriority(t Level2EventType) int {
	switch t {
	case Level2RouteLeak, Level2Outage:
		return 3 // Critical
	case Level2LinkFlap, Level2Babbling, Level2NextHopOscillation, Level2AggFlap:
		return 2 // Bad
	case Level2PolicyChurn, Level2PathLengthOscillation, Level2PathHunting:
		return 1 // Normal
	default:
		return 0 // Discovery
	}
}

func (c *BGPClassifier) GetStats() (stats map[Level2EventType]int, totalEvents int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	statsCopy := make(map[Level2EventType]int)
	for k, v := range c.level2Stats {
		statsCopy[k] = v
	}

	return statsCopy, c.totalLevel2Events
}
