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
	"google.golang.org/protobuf/proto"
)

type LeakType int

const (
	LeakUnknown LeakType = iota
	LeakHairpin
	LeakLateral
	LeakProviderToPeer
	LeakPeerToProvider
	LeakReOrigination
)

const (
	strUnknown = "Unknown"
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
	default:
		return strUnknown
	}
}

type LeakDetail struct {
	Type      LeakType
	LeakerASN uint32
	VictimASN uint32
}

type ClassificationType int

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
	ClassificationNone ClassificationType = iota
	ClassificationLinkFlap
	ClassificationAggFlap
	ClassificationPathLengthOscillation
	ClassificationBabbling
	ClassificationPathHunting
	ClassificationPolicyChurn
	ClassificationNextHopOscillation
	ClassificationOutage
	ClassificationRouteLeak
	ClassificationDiscovery
)

func (t ClassificationType) String() string {
	switch t {
	case ClassificationLinkFlap:
		return nameLinkFlap
	case ClassificationAggFlap:
		return nameAggFlap
	case ClassificationPathLengthOscillation:
		return namePathOscillation
	case ClassificationBabbling:
		return nameBabbling
	case ClassificationPathHunting:
		return namePathHunting
	case ClassificationPolicyChurn:
		return namePolicyChurn
	case ClassificationNextHopOscillation:
		return nameNextHopFlap
	case ClassificationOutage:
		return nameHardOutage
	case ClassificationRouteLeak:
		return nameRouteLeak
	case ClassificationDiscovery:
		return nameDiscovery
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

type Classifier struct {
	seenDB     *utils.DiskTrie
	stateDB    *utils.DiskTrie
	asnMapping *utils.ASNMapping
	rpki       *utils.RPKIManager
	prefixToIP PrefixToIPConverter

	classificationStats          map[ClassificationType]int
	classificationUniquePrefixes map[ClassificationType]map[string]struct{}
	totalClassificationEvents    int
	prefixStates                 *utils.LRUCache[string, *bgpproto.PrefixState]
	timeProvider                 TimeProvider

	mu sync.Mutex
}

func NewClassifier(seenDB, stateDB *utils.DiskTrie, asnMapping *utils.ASNMapping, rpki *utils.RPKIManager, prefixToIP PrefixToIPConverter, prefixStates *utils.LRUCache[string, *bgpproto.PrefixState], timeProvider TimeProvider) *Classifier {
	return &Classifier{
		seenDB:                       seenDB,
		stateDB:                      stateDB,
		asnMapping:                   asnMapping,
		rpki:                         rpki,
		prefixToIP:                   prefixToIP,
		classificationStats:          make(map[ClassificationType]int),
		classificationUniquePrefixes: make(map[ClassificationType]map[string]struct{}),
		prefixStates:                 prefixStates,
		timeProvider:                 timeProvider,
	}
}

func (c *Classifier) GetPrefixState(prefix string) (*bgpproto.PrefixState, bool) {
	return c.prefixStates.Get(prefix)
}

func (c *Classifier) ClassifyEvent(prefix string, ctx *MessageContext) (pendingEvent, bool) {
	if strings.Contains(prefix, ":") {
		return pendingEvent{}, false
	}
	state, ok := c.prefixStates.Get(prefix)
	if !ok {
		// Try to load from stateDB
		if c.stateDB != nil {
			data, err := c.stateDB.Get(prefix)
			if err == nil && data != nil {
				state = &bgpproto.PrefixState{}
				if err := proto.Unmarshal(data, state); err != nil {
					log.Printf("Error unmarshaling prefix state: %v", err)
					state = nil
				}
			}
		}

		if state == nil {
			state = &bgpproto.PrefixState{
				PeerLastAttrs: make(map[string]*bgpproto.LastAttrs),
				Buckets:       make(map[int64]*bgpproto.StatsBucket),
				StartTimeTs:   ctx.Now.Unix(),
			}
		}
		c.prefixStates.Add(prefix, state)
	}

	state.LastUpdateTs = ctx.Now.Unix()
	bucket := c.getOrCreateBucket(state, ctx.Now)
	bucket.TotalMessages++

	if ctx.IsWithdrawal {
		bucket.Withdrawals++
	} else {
		// We always update peer attributes for consensus tracking
		c.updateAnnouncementStats(state, bucket, ctx)
		if c.rpki != nil && ctx.OriginASN != 0 {
			status, err := c.rpki.Validate(prefix, ctx.OriginASN)
			if err == nil {
				state.LastRpkiStatus = int32(status)
				state.LastOriginAsn = ctx.OriginASN
			}
		}
	}

	ctx.LastRpkiStatus = state.LastRpkiStatus
	ctx.LastOriginAsn = state.LastOriginAsn

	// If already classified, emit the classification pulse immediately for this peer
	if state.ClassifiedType != 0 {
		if ctx.Now.Unix()-state.ClassifiedTimeTs > 600 {
			state.ClassifiedType = 0
			state.ClassifiedTimeTs = 0
			state.UncategorizedCounted = false
		} else if ClassificationType(state.ClassifiedType) != ClassificationDiscovery &&
			ClassificationType(state.ClassifiedType) != ClassificationPolicyChurn &&
			ClassificationType(state.ClassifiedType) != ClassificationPathHunting &&
			ClassificationType(state.ClassifiedType) != ClassificationPathLengthOscillation {
			// If it's a Normal anomaly, we still allow upgrade to Bad/Critical
			var ld *LeakDetail
			if state.LeakType != 0 {
				ld = &LeakDetail{
					Type:      LeakType(state.LeakType),
					LeakerASN: state.LeakerAsn,
					VictimASN: state.VictimAsn,
				}
			}
			return pendingEvent{
				ip:                 c.prefixToIP(prefix),
				prefix:             prefix,
				asn:                ctx.OriginASN,
				eventType:          ctx.EventType(),
				classificationType: ClassificationType(state.ClassifiedType),
				leakDetail:         ld,
			}, true
		}
	}

	return c.evaluatePrefixState(prefix, state, ctx)
}

func (c *Classifier) getOrCreateBucket(state *bgpproto.PrefixState, now time.Time) *bgpproto.StatsBucket {
	minuteTS := now.Truncate(time.Minute).Unix()
	if state.Buckets == nil {
		state.Buckets = make(map[int64]*bgpproto.StatsBucket)
	}
	bucket, ok := state.Buckets[minuteTS]
	if !ok {
		bucket = &bgpproto.StatsBucket{}
		state.Buckets[minuteTS] = bucket
	}

	// Also cleanup old buckets here to keep memory/size low
	cutoff := now.Add(-10 * time.Minute).Unix()
	for ts := range state.Buckets {
		if ts < cutoff {
			delete(state.Buckets, ts)
		}
	}

	return bucket
}

func (c *Classifier) updateAnnouncementStats(state *bgpproto.PrefixState, bucket *bgpproto.StatsBucket, ctx *MessageContext) {
	bucket.Announcements++

	peer := ctx.Peer
	if state.PeerLastAttrs == nil {
		state.PeerLastAttrs = make(map[string]*bgpproto.LastAttrs)
	}

	if last, ok := state.PeerLastAttrs[peer]; ok {
		c.compareAndUpdateBucketStats(bucket, last, ctx)
	}

	state.PeerLastAttrs[peer] = &bgpproto.LastAttrs{
		Path:         ctx.PathStr,
		Communities:  ctx.CommStr,
		NextHop:      ctx.NextHop,
		Aggregator:   ctx.Aggregator,
		LastPathLen:  int32(ctx.PathLen),
		OriginAsn:    ctx.OriginASN,
		Med:          ctx.Med,
		LocalPref:    ctx.LocalPref,
		LastUpdateTs: ctx.Now.Unix(),
		Host:         ctx.Host,
	}
}

func (c *Classifier) compareAndUpdateBucketStats(bucket *bgpproto.StatsBucket, last *bgpproto.LastAttrs, ctx *MessageContext) {
	if ctx.PathStr != last.Path {
		bucket.PathChanges++
	}
	if ctx.CommStr != last.Communities {
		bucket.CommunityChanges++
	}
	if ctx.NextHop != last.NextHop {
		bucket.NextHopChanges++
	}
	if ctx.Aggregator != last.Aggregator {
		bucket.AggregatorChanges++
	}
	if ctx.Med != last.Med {
		bucket.MedChanges++
	}
	if ctx.LocalPref != last.LocalPref {
		bucket.LocalPrefChanges++
	}
	if int32(ctx.PathLen) != last.LastPathLen && last.LastPathLen != 0 {
		if int32(ctx.PathLen) > last.LastPathLen {
			bucket.PathLengthIncreases++
		} else {
			bucket.PathLengthDecreases++
		}
	}
}

func (c *Classifier) evaluatePrefixState(prefix string, state *bgpproto.PrefixState, ctx *MessageContext) (pendingEvent, bool) {
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
		return pendingEvent{}, false
	}

	anomType, leakDetail, classified := c.findClassification(prefix, state, &stats, elapsed, ctx)

	if classified {
		if state.ClassifiedType != 0 {
			if c.getPriority(anomType) <= c.getPriority(ClassificationType(state.ClassifiedType)) {
				// We already have a classification of equal or higher priority
				// Just return the event for the current classification
				var ld *LeakDetail
				if state.LeakType != 0 {
					ld = &LeakDetail{
						Type:      LeakType(state.LeakType),
						LeakerASN: state.LeakerAsn,
						VictimASN: state.VictimAsn,
					}
				}
				return pendingEvent{
					ip:                 c.prefixToIP(prefix),
					prefix:             prefix,
					asn:                ctx.OriginASN,
					eventType:          ctx.EventType(),
					classificationType: ClassificationType(state.ClassifiedType),
					leakDetail:         ld,
				}, true
			}
		}
		return c.RecordClassification(prefix, state, anomType, ctx.Now.Unix(), ctx, leakDetail), true
	}
	return pendingEvent{}, false
}

func (c *Classifier) getPriority(t ClassificationType) int {
	switch t {
	case ClassificationRouteLeak, ClassificationOutage:
		return 3 // Critical
	case ClassificationLinkFlap, ClassificationBabbling, ClassificationNextHopOscillation, ClassificationAggFlap:
		return 2 // Bad
	case ClassificationPolicyChurn, ClassificationPathLengthOscillation, ClassificationPathHunting:
		return 1 // Normal
	default:
		return 0 // Discovery
	}
}

func (c *Classifier) aggregateRecentBuckets(state *bgpproto.PrefixState, now time.Time, currentOriginASN uint32) prefixStats {
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

func (c *Classifier) findClassification(prefix string, state *bgpproto.PrefixState, s *prefixStats, elapsed float64, ctx *MessageContext) (ClassificationType, *LeakDetail, bool) {
	numPeers := float64(len(state.PeerLastAttrs))
	if numPeers == 0 {
		numPeers = 1
	}
	perPeerRate := float64(s.totalMsgs) / numPeers

	// 1. Critical
	if et, ld, ok := c.findCriticalAnomaly(prefix, s, elapsed, ctx); ok {
		return et, ld, true
	}

	// 2. Bad
	if et, ok := c.findBadAnomaly(s, elapsed, perPeerRate); ok {
		return et, nil, true
	}

	// 3. Normal / Policy
	if et, ok := c.findNormalAnomaly(s, elapsed); ok {
		return et, nil, true
	}

	return ClassificationNone, nil, false
}

func (c *Classifier) getHistoricalASN(prefix string) uint32 {
	if c.seenDB == nil {
		return 0
	}
	val, _ := c.seenDB.Get(prefix)
	if len(val) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(val)
}

func (c *Classifier) findCriticalAnomaly(prefix string, s *prefixStats, elapsed float64, ctx *MessageContext) (ClassificationType, *LeakDetail, bool) {
	if s.totalWith >= 3 && s.totalAnn == 0 && elapsed > 60 {
		return ClassificationOutage, nil, true
	}

	peerCount := len(s.uniquePeers)
	hostCount := len(s.uniqueHosts)

	// 1. Hijack Detection (RPKI Signal)
	if anom, ld, ok := c.detectHijack(prefix, peerCount, hostCount, ctx); ok {
		return anom, ld, true
	}

	// 2. Route Leak Detection (AS Path Heuristic)
	if anom, ld, ok := c.detectRouteLeak(prefix, peerCount, hostCount, ctx); ok {
		return anom, ld, true
	}

	return ClassificationNone, nil, false
}

func (c *Classifier) detectHijack(prefix string, peerCount, hostCount int, ctx *MessageContext) (ClassificationType, *LeakDetail, bool) {
	if ctx.OriginASN == 0 || utils.RPKIStatus(ctx.LastRpkiStatus) != utils.RPKIInvalidASN {
		return ClassificationNone, nil, false
	}

	if c.isDDoSProvider(ctx.OriginASN) {
		return ClassificationNone, nil, false
	}

	// Historical Origin Check (The Filter)
	historicalASN := c.getHistoricalASN(prefix)
	expectedASN := historicalASN
	if expectedASN == 0 && c.rpki != nil {
		expectedASN = c.rpki.GetExpectedASN(prefix)
	}

	// Transition Hijack (Highest Signal)
	isTransition := historicalASN != 0 && historicalASN != ctx.OriginASN
	if isTransition {
		if c.isSibling(ctx.OriginASN, historicalASN) {
			return ClassificationNone, nil, false
		}

		if peerCount >= 3 && hostCount >= 2 {
			nameNew := strUnknown
			namePrev := strUnknown
			if c.asnMapping != nil {
				nameNew = c.asnMapping.GetName(ctx.OriginASN)
				namePrev = c.asnMapping.GetName(historicalASN)
			}
			log.Printf("[!!! HIJACK TRANSITION !!!] Prefix: %s, New Origin: AS%d (%s), Prev Origin: AS%d (%s), RPKI: InvalidASN, Consensus: %d peers/%d hosts",
				prefix, ctx.OriginASN, nameNew, historicalASN, namePrev, peerCount, hostCount)
			return ClassificationRouteLeak, &LeakDetail{
				Type:      LeakReOrigination,
				LeakerASN: ctx.OriginASN,
				VictimASN: historicalASN,
			}, true
		}
	}

	// New Prefix Hijack (RPKI Invalid but never seen before)
	if historicalASN == 0 {
		if expectedASN != 0 && c.isSibling(ctx.OriginASN, expectedASN) {
			return ClassificationNone, nil, false
		}

		// Require VERY high consensus for brand new prefixes being invalid
		if peerCount >= 15 && hostCount >= 5 {
			nameLeaker := strUnknown
			nameVictim := strUnknown
			if c.asnMapping != nil {
				nameLeaker = c.asnMapping.GetName(ctx.OriginASN)
				if expectedASN != 0 {
					nameVictim = c.asnMapping.GetName(expectedASN)
				}
			}
			log.Printf("[!!! HIJACK NEW PREFIX !!!] Prefix: %s, Origin: AS%d (%s), RPKI: InvalidASN, Expected Origin: AS%d (%s), Consensus: %d peers/%d hosts",
				prefix, ctx.OriginASN, nameLeaker, expectedASN, nameVictim, peerCount, hostCount)
			return ClassificationRouteLeak, &LeakDetail{
				Type:      LeakReOrigination,
				LeakerASN: ctx.OriginASN,
				VictimASN: expectedASN,
			}, true
		}
	}

	return ClassificationNone, nil, false
}

func (c *Classifier) detectRouteLeak(prefix string, peerCount, hostCount int, ctx *MessageContext) (ClassificationType, *LeakDetail, bool) {
	ld, ok := c.hasRouteLeak(ctx)
	if !ok {
		return ClassificationNone, nil, false
	}

	// Consensus requirement for path violations to filter out terminal edge/collector leaks.
	if peerCount >= 3 && hostCount >= 2 {
		c.logRouteLeak(prefix, ld)
		return ClassificationRouteLeak, ld, true
	}

	return ClassificationNone, nil, false
}

func (c *Classifier) isDDoSProvider(asn uint32) bool {
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
		262287: true, // Latitude.sh
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

func (c *Classifier) isSibling(asn1, asn2 uint32) bool {
	if asn1 == asn2 {
		return true
	}
	if c.asnMapping == nil {
		return false
	}

	org1 := c.asnMapping.GetOrgID(asn1)
	org2 := c.asnMapping.GetOrgID(asn2)
	if org1 != "" && org1 == org2 {
		return true
	}

	name1 := c.asnMapping.GetName(asn1)
	name2 := c.asnMapping.GetName(asn2)

	// Only use heuristic if both names are reasonable length (e.g. not just "A") and both are valid
	if name1 != strUnknown && name2 != strUnknown {
		n1 := strings.Fields(strings.ToLower(name1))
		n2 := strings.Fields(strings.ToLower(name2))

		// If the names are exactly the same, they are siblings
		if strings.EqualFold(name1, name2) {
			return true
		}

		if len(n1) > 0 && len(n2) > 0 {
			w1 := strings.TrimRight(n1[0], ",")
			w2 := strings.TrimRight(n2[0], ",")

			// Try splitting by hyphen as well
			if strings.Contains(w1, "-") {
				w1 = strings.Split(w1, "-")[0]
			}
			if strings.Contains(w2, "-") {
				w2 = strings.Split(w2, "-")[0]
			}

			if w1 == w2 && len(w1) > 3 { // Ensure the matched word is meaningful
				return true
			}
		}
	}
	return false
}

func (c *Classifier) hasRouteLeak(ctx *MessageContext) (*LeakDetail, bool) {
	if ctx.PathStr == "" {
		return nil, false
	}
	// Basic path parser (path is stored as string "[1 2 3]")
	fields := strings.Fields(strings.Trim(ctx.PathStr, "[]"))
	if len(fields) < 3 {
		return nil, false
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
		return nil, false
	}

	path := make([]uint32, 0, len(rawPath))
	path = append(path, rawPath[0])

	for i := 1; i < len(rawPath); i++ {
		// Only add if it's NOT a sibling ASN to the last one added
		if !c.isSibling(rawPath[i], path[len(path)-1]) {
			path = append(path, rawPath[i])
		}
	}

	if len(path) < 3 {
		return nil, false
	}

	// 2. Valley-free violation check on collapsed path
	for i := 0; i < len(path)-2; i++ {
		p1, p2, p3 := path[i], path[i+1], path[i+2]

		if c.isSibling(p1, p3) {
			continue
		}

		// Hairpin turn: Tier-1 -> Non-Tier-1 -> Tier-1
		if c.isTier1(p1) && !c.isLargeNetwork(p2) && !c.isCloud(p2) && c.isTier1(p3) {
			return &LeakDetail{
				Type:      LeakHairpin,
				LeakerASN: p2,
				VictimASN: p3,
			}, true
		}

		// lateral infection: Peer -> Stub -> Peer
		// (Using isLargeNetwork as a proxy for "network with peering relationships")
		// If both ends are large and middle is small, it's potentially a leak.
		// We'll refine this by checking if they are not clouds.
		if !c.isLargeNetwork(p2) && !c.isCloud(p2) {
			if c.isLargeNetwork(p1) && c.isLargeNetwork(p3) {
				return &LeakDetail{Type: LeakLateral, LeakerASN: p2, VictimASN: p3}, true
			}
		}
	}

	// 3. Provider-to-Peer / Peer-to-Provider (Heuristics)
	// These require more complex relationship data than we have here,
	// but we can catch some via AS-Path length increases and Tier-1 presence.
	return nil, false
}

func (c *Classifier) logRouteLeak(prefix string, ld *LeakDetail) {
	nameLeaker := strUnknown
	nameVictim := strUnknown
	if c.asnMapping != nil {
		nameLeaker = c.asnMapping.GetName(ld.LeakerASN)
		if ld.VictimASN != 0 {
			nameVictim = c.asnMapping.GetName(ld.VictimASN)
		}
	}
	log.Printf("[!!! ROUTE LEAK (%s) !!!] Prefix: %s, Leaker: AS%d (%s), Victim: AS%d (%s)",
		ld.Type.String(), prefix, ld.LeakerASN, nameLeaker, ld.VictimASN, nameVictim)
}

func (c *Classifier) isTier1(asn uint32) bool {
	switch asn {
	case 209, 701, 702, 1239, 1299, 2828, 2914, 3257, 3320, 3356, 3491, 3549, 3561, 5511, 6453, 6461, 6762, 6830, 7018, 12956: // Global Tier-1s
		return true
	default:
		return false
	}
}

func (c *Classifier) isLargeNetwork(asn uint32) bool {
	if c.isTier1(asn) {
		return true
	}
	switch asn {
	case 174, 6939, 9002, 1273, 4637, 7922, 4134, 4809, 4837, 7473, 9808: // Major Regional/National Backbones
		return true
	default:
		return false
	}
}

func (c *Classifier) isCloud(asn uint32) bool {
	switch asn {
	case 13335, 15169, 16509, 14618, 20940, 8075, 32934, 31898, 40027, 36040, 54113, 14061, 37963, 45102, 16625: // Major Cloud/CDN
		return true
	default:
		return false
	}
}

func (c *Classifier) findBadAnomaly(s *prefixStats, elapsed, perPeerRate float64) (ClassificationType, bool) {
	isAggFlap := s.totalAgg >= 10 && float64(s.totalAgg)/elapsed > 0.05
	isNextHopOsc := len(s.uniqueHops) > 1 && s.totalHop >= 10 && s.totalPath <= 2
	isLinkFlap := s.totalWith >= 5 && float64(s.totalAnn)/float64(s.totalWith) < 2.0

	if isAggFlap {
		return ClassificationAggFlap, true
	}
	if isNextHopOsc {
		return ClassificationNextHopOscillation, true
	}
	if isLinkFlap {
		return ClassificationLinkFlap, true
	}

	// Babbling is the catch-all for "Bad" high volume activity
	if perPeerRate >= 2.0 && s.totalMsgs >= 20 && s.totalPath == 0 && s.totalComm == 0 && s.totalMed == 0 && s.totalLP == 0 && s.totalAgg == 0 && s.totalHop == 0 {
		return ClassificationBabbling, true
	}
	return ClassificationNone, false
}

func (c *Classifier) findNormalAnomaly(s *prefixStats, elapsed float64) (ClassificationType, bool) {
	isPathHunting := s.totalAnn >= 5 && s.totalIncreases >= 2 && s.totalWith >= 1
	isPolicyChurn := s.totalComm >= 10 || (s.totalPath >= 10 && s.totalIncreases+s.totalDecreases <= 2) || (s.totalMed+s.totalLP >= 5 && s.totalPath <= 5)
	isPathLengthOsc := (s.totalIncreases+s.totalDecreases) >= 5 && float64(s.totalIncreases+s.totalDecreases)/elapsed > 0.01

	if isPathHunting {
		return ClassificationPathHunting, true
	}
	if isPolicyChurn {
		return ClassificationPolicyChurn, true
	}
	if isPathLengthOsc {
		return ClassificationPathLengthOscillation, true
	}

	// Discovery as the catch-all for high volume activity (>= 25 messages)
	// that didn't match any "Bad" anomaly or specific "Normal" pattern.
	if s.totalMsgs >= 25 {
		return ClassificationDiscovery, true
	}
	return ClassificationNone, false
}

func (c *Classifier) GetRPKIManager() *utils.RPKIManager {
	return c.rpki
}

func (c *Classifier) GetASNMapping() *utils.ASNMapping {
	return c.asnMapping
}

func (c *Classifier) RecordClassification(prefix string, state *bgpproto.PrefixState, anomType ClassificationType, now int64, ctx *MessageContext, leakDetail ...*LeakDetail) pendingEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.classificationStats[anomType]++
	if c.classificationUniquePrefixes[anomType] == nil {
		c.classificationUniquePrefixes[anomType] = make(map[string]struct{})
	}
	c.classificationUniquePrefixes[anomType][prefix] = struct{}{}
	c.totalClassificationEvents++

	// Get an origin ASN for visualization
	originASN := uint32(0)
	for _, attr := range state.PeerLastAttrs {
		if attr.OriginAsn != 0 {
			originASN = attr.OriginAsn
			break
		}
	}
	if originASN == 0 {
		originASN = ctx.OriginASN
	}

	// Record that this prefix is now classified
	state.ClassifiedType = int32(anomType)
	state.ClassifiedTimeTs = now

	var ld *LeakDetail
	if len(leakDetail) > 0 {
		ld = leakDetail[0]
	}

	if ld != nil {
		state.LeakType = int32(ld.Type)
		state.LeakerAsn = ld.LeakerASN
		state.VictimAsn = ld.VictimASN
	}

	// Persist state if we have a stateDB
	if c.stateDB != nil {
		if data, err := proto.Marshal(state); err == nil {
			_ = c.stateDB.Put(prefix, data)
		}
	}

	return pendingEvent{
		ip:                 c.prefixToIP(prefix),
		prefix:             prefix,
		asn:                originASN,
		eventType:          ctx.EventType(),
		classificationType: anomType,
		leakDetail:         ld,
	}
}

func (c *Classifier) GetClassificationStats() (stats map[ClassificationType]int, totalEvents int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	statsCopy := make(map[ClassificationType]int)
	for k, v := range c.classificationStats {
		statsCopy[k] = v
	}

	return statsCopy, c.totalClassificationEvents
}
