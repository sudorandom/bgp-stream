package bgp

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgp/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/utils"
	"google.golang.org/protobuf/proto"
)

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
	withdrawnPeers                           map[string]bool
	withdrawnHosts                           map[string]bool
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

func (c *Classifier) ClassifyEvent(prefix string, ctx *MessageContext) (PendingEvent, bool) {
	if strings.Contains(prefix, ":") {
		return PendingEvent{}, false
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

	historicalOriginAsn := state.LastOriginAsn
	if historicalOriginAsn == 0 {
		historicalOriginAsn = c.getHistoricalASN(prefix)
	}

	if ctx.IsWithdrawal {
		c.handleWithdrawal(state, bucket, ctx)
	} else {
		// We always update peer attributes for consensus tracking
		c.updateAnnouncementStats(state, bucket, ctx)
		c.updateRPKIStatus(prefix, state, ctx)
	}

	ctx.LastRpkiStatus = state.LastRpkiStatus
	ctx.LastOriginAsn = state.LastOriginAsn

	// If already classified, emit the classification pulse immediately for this peer
	if state.ClassifiedType != 0 {
		// Recovery check: If it was an outage but we are seeing announcements now, reset classification
		switch {
		case ClassificationType(state.ClassifiedType) == ClassificationOutage && !ctx.IsWithdrawal:
			state.ClassifiedType = 0
			state.ClassifiedTimeTs = 0
			state.UncategorizedCounted = false
		case ctx.Now.Unix()-state.ClassifiedTimeTs > 600:
			state.ClassifiedType = 0
			state.ClassifiedTimeTs = 0
			state.UncategorizedCounted = false
		default:
			// Always emit updates for ongoing classifications to keep them active in the stream
			var ld *LeakDetail
			if state.LeakType != 0 || ClassificationType(state.ClassifiedType) == ClassificationDDoSMitigation {
				ld = &LeakDetail{
					Type:      LeakType(state.LeakType),
					LeakerASN: state.LeakerAsn,
					VictimASN: state.VictimAsn,
				}
			}
			return PendingEvent{
				IP:                 c.prefixToIP(prefix),
				Prefix:             prefix,
				ASN:                ctx.OriginASN,
				HistoricalASN:      historicalOriginAsn,
				EventType:          ctx.EventType(),
				ClassificationType: ClassificationType(state.ClassifiedType),
				LeakDetail:         ld,
			}, true
		}
	}

	return c.evaluatePrefixState(prefix, state, historicalOriginAsn, ctx)
}

func (c *Classifier) handleWithdrawal(state *bgpproto.PrefixState, bucket *bgpproto.StatsBucket, ctx *MessageContext) {
	bucket.Withdrawals++
	sessionKey := ctx.Host + ":" + ctx.Peer
	if state.PeerLastAttrs == nil {
		state.PeerLastAttrs = make(map[string]*bgpproto.LastAttrs)
	}
	if last, ok := state.PeerLastAttrs[sessionKey]; ok {
		last.Withdrawn = true
		last.LastUpdateTs = ctx.Now.Unix()
	} else {
		state.PeerLastAttrs[sessionKey] = &bgpproto.LastAttrs{
			Withdrawn:    true,
			LastUpdateTs: ctx.Now.Unix(),
			Host:         ctx.Host,
		}
	}
}

func (c *Classifier) updateRPKIStatus(prefix string, state *bgpproto.PrefixState, ctx *MessageContext) {
	if c.rpki != nil && ctx.OriginASN != 0 {
		status, err := c.rpki.Validate(prefix, ctx.OriginASN)
		if err == nil {
			state.LastRpkiStatus = int32(status)
			state.LastOriginAsn = ctx.OriginASN
		}
	} else if ctx.LastRpkiStatus != 0 {
		state.LastRpkiStatus = ctx.LastRpkiStatus
		state.LastOriginAsn = ctx.OriginASN
	}
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

	sessionKey := ctx.Host + ":" + ctx.Peer
	if state.PeerLastAttrs == nil {
		state.PeerLastAttrs = make(map[string]*bgpproto.LastAttrs)
	}

	if last, ok := state.PeerLastAttrs[sessionKey]; ok {
		c.compareAndUpdateBucketStats(bucket, last, ctx)
	}

	state.PeerLastAttrs[sessionKey] = &bgpproto.LastAttrs{
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
		Withdrawn:    false,
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

func (c *Classifier) evaluatePrefixState(prefix string, state *bgpproto.PrefixState, historicalOriginAsn uint32, ctx *MessageContext) (PendingEvent, bool) {
	stats := c.aggregateRecentBuckets(state, ctx.Now, ctx.OriginASN)

	elapsed := float64(ctx.Now.Unix() - stats.earliestTS)
	if elapsed < 60 {
		elapsed = float64(ctx.Now.Unix() - state.StartTimeTs)
	}
	if elapsed <= 0 {
		elapsed = 1
	}

	// Ensure we have seen enough messages over a small time window to classify
	if elapsed < 60 && stats.totalMsgs < 5 {
		return PendingEvent{}, false
	}

	anomType, leakDetail, classified := c.findClassification(prefix, &stats, elapsed, ctx)

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
				if ClassificationType(state.ClassifiedType) == ClassificationDDoSMitigation && state.VictimAsn == 0 {
					if ld == nil {
						ld = &LeakDetail{}
					}
					ld.VictimASN = historicalOriginAsn
				}
				return PendingEvent{
					IP:                 c.prefixToIP(prefix),
					Prefix:             prefix,
					ASN:                ctx.OriginASN,
					HistoricalASN:      historicalOriginAsn,
					EventType:          ctx.EventType(),
					ClassificationType: ClassificationType(state.ClassifiedType),
					LeakDetail:         ld,
				}, true
			}
		}
		return c.RecordClassification(prefix, state, anomType, ctx.Now.Unix(), ctx, historicalOriginAsn, leakDetail), true
	}
	return PendingEvent{}, false
}

func (c *Classifier) getPriority(t ClassificationType) int {
	switch t {
	case ClassificationRouteLeak, ClassificationOutage, ClassificationDDoSMitigation, ClassificationHijack, ClassificationBogon:
		return 3 // Critical
	case ClassificationFlap:
		return 2 // Bad
	case ClassificationTrafficEngineering, ClassificationPathHunting:
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
		earliestTS:     now.Unix(),
		uniqueHops:     make(map[string]bool),
		uniqueASNs:     make(map[uint32]bool),
		uniquePeers:    make(map[string]bool),
		uniqueHosts:    make(map[string]bool),
		withdrawnPeers: make(map[string]bool),
		withdrawnHosts: make(map[string]bool),
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
		// Count withdrawn peers/hosts (regardless of origin ASN)
		if attr.Withdrawn {
			s.withdrawnPeers[peer] = true
			if attr.Host != "" {
				s.withdrawnHosts[attr.Host] = true
			}
		} else if attr.OriginAsn == currentOriginASN {
			// Only count active peers and hosts that are currently seeing the same origin ASN
			s.uniquePeers[peer] = true
			if attr.Host != "" {
				s.uniqueHosts[attr.Host] = true
			}
		}
	}
	return s
}

func (c *Classifier) findClassification(prefix string, s *prefixStats, elapsed float64, ctx *MessageContext) (ClassificationType, *LeakDetail, bool) {
	// 1. Critical
	if et, ld, ok := c.findCriticalAnomaly(prefix, s, elapsed, ctx); ok {
		return et, ld, true
	}

	// 2. Bad
	if et, ok := c.findBadAnomaly(s); ok {
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
	// Try exact match first
	val, _ := c.seenDB.Get(prefix)
	if len(val) >= 4 {
		return binary.BigEndian.Uint32(val)
	}

	// Fallback to longest prefix match (LPM) if it's a new prefix (like /32)
	ipStr := prefix
	if strings.Contains(prefix, "/") {
		ipStr = strings.Split(prefix, "/")[0]
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return 0
	}
	val, _, err := c.seenDB.Lookup(ip)
	if err == nil && len(val) >= 4 {
		return binary.BigEndian.Uint32(val)
	}
	return 0
}

func (c *Classifier) findCriticalAnomaly(prefix string, s *prefixStats, elapsed float64, ctx *MessageContext) (ClassificationType, *LeakDetail, bool) {
	peerCount := len(s.uniquePeers)
	hostCount := len(s.uniqueHosts)
	withdrawnPeerCount := len(s.withdrawnPeers)
	withdrawnHostCount := len(s.withdrawnHosts)
	totalKnownPeers := peerCount + withdrawnPeerCount

	// 0. Bogon Detection
	if c.isBogon(prefix, ctx) {
		return ClassificationBogon, nil, true
	}

	// Outage heuristic based on host diversity and total peers tracking the prefix
	// Industry standard: A prefix is considered in outage if it loses all its paths (peerCount == 0)
	if elapsed > 60 && totalKnownPeers > 0 && peerCount == 0 && withdrawnPeerCount > 0 {
		if withdrawnPeerCount >= 3 && withdrawnHostCount >= 2 {
			// Sufficient diversity across collectors and peers to confirm an outage
			return ClassificationOutage, nil, true
		} else if withdrawnPeerCount >= totalKnownPeers && withdrawnHostCount >= 2 {
			// For smaller prefixes (<= 2 peers), require all known peers and multiple hosts to have withdrawn
			return ClassificationOutage, nil, true
		}
	}

	// 0.5 DDoS Mitigation Detection
	if ld, ok := c.detectDDoSMitigation(prefix, ctx); ok {
		return ClassificationDDoSMitigation, ld, true
	}

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
	if ctx.OriginASN == 0 || (utils.RPKIStatus(ctx.LastRpkiStatus) != utils.RPKIInvalidASN && utils.RPKIStatus(ctx.LastRpkiStatus) != utils.RPKIInvalidMaxLength) {
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
		return c.detectTransitionHijack(prefix, peerCount, hostCount, ctx.OriginASN, historicalASN)
	}

	// New Prefix Hijack (RPKI Invalid but never seen before)
	if historicalASN == 0 {
		return c.detectNewPrefixHijack(prefix, peerCount, hostCount, ctx.OriginASN, expectedASN)
	}

	return ClassificationNone, nil, false
}

func (c *Classifier) detectTransitionHijack(prefix string, peerCount, hostCount int, originASN, historicalASN uint32) (ClassificationType, *LeakDetail, bool) {
	if c.isSibling(originASN, historicalASN) {
		return ClassificationNone, nil, false
	}

	if peerCount >= 3 && hostCount >= 2 {
		nameNew := StrUnknown
		namePrev := StrUnknown
		if c.asnMapping != nil {
			nameNew = c.asnMapping.GetName(originASN)
			namePrev = c.asnMapping.GetName(historicalASN)
		}
		log.Printf("[!!! HIJACK TRANSITION !!!] Prefix: %s, New Origin: AS%d (%s), Prev Origin: AS%d (%s), RPKI: InvalidASN, Consensus: %d peers/%d hosts",
			prefix, originASN, nameNew, historicalASN, namePrev, peerCount, hostCount)
		return ClassificationHijack, &LeakDetail{
			Type:      LeakReOrigination,
			LeakerASN: originASN,
			VictimASN: historicalASN,
		}, true
	}
	return ClassificationNone, nil, false
}

func (c *Classifier) detectNewPrefixHijack(prefix string, peerCount, hostCount int, originASN, expectedASN uint32) (ClassificationType, *LeakDetail, bool) {
	if expectedASN != 0 && c.isSibling(originASN, expectedASN) {
		return ClassificationNone, nil, false
	}

	// Require VERY high consensus for brand new prefixes being invalid
	if peerCount >= 15 && hostCount >= 5 {
		nameLeaker := StrUnknown
		nameVictim := StrUnknown
		if c.asnMapping != nil {
			nameLeaker = c.asnMapping.GetName(originASN)
			if expectedASN != 0 {
				nameVictim = c.asnMapping.GetName(expectedASN)
			}
		}
		log.Printf("[!!! HIJACK NEW PREFIX !!!] Prefix: %s, Origin: AS%d (%s), RPKI: InvalidASN, Expected Origin: AS%d (%s), Consensus: %d peers/%d hosts",
			prefix, originASN, nameLeaker, expectedASN, nameVictim, peerCount, hostCount)
		return ClassificationHijack, &LeakDetail{
			Type:      LeakReOrigination,
			LeakerASN: originASN,
			VictimASN: expectedASN,
		}, true
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
	if name1 != StrUnknown && name2 != StrUnknown {
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
	nameLeaker := StrUnknown
	nameVictim := StrUnknown
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

func (c *Classifier) findBadAnomaly(s *prefixStats) (ClassificationType, bool) {
	isNextHopOsc := len(s.uniqueHops) > 1 && s.totalHop >= 10 && s.totalPath <= 2
	isLinkFlap := s.totalWith >= 5 && float64(s.totalAnn)/float64(s.totalWith) < 2.0

	if isNextHopOsc || isLinkFlap {
		return ClassificationFlap, true
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
	if isPolicyChurn || isPathLengthOsc {
		return ClassificationTrafficEngineering, true
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

func (c *Classifier) RecordClassification(prefix string, state *bgpproto.PrefixState, anomType ClassificationType, now int64, ctx *MessageContext, historicalOriginAsn uint32, leakDetail ...*LeakDetail) PendingEvent {
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

	if anomType == ClassificationDDoSMitigation {
		if state.VictimAsn == 0 {
			state.VictimAsn = historicalOriginAsn
		}
		if state.LeakerAsn == 0 {
			state.LeakerAsn = originASN
		}
		if ld == nil {
			ld = &LeakDetail{
				LeakerASN: state.LeakerAsn,
				VictimASN: state.VictimAsn,
			}
		} else {
			if ld.LeakerASN == 0 {
				ld.LeakerASN = state.LeakerAsn
			}
			if ld.VictimASN == 0 {
				ld.VictimASN = state.VictimAsn
			}
		}
	}

	// Persist state if we have a stateDB
	if c.stateDB != nil {
		if data, err := proto.Marshal(state); err == nil {
			_ = c.stateDB.Put(prefix, data)
		}
	}

	return PendingEvent{
		IP:                 c.prefixToIP(prefix),
		Prefix:             prefix,
		ASN:                originASN,
		HistoricalASN:      historicalOriginAsn,
		EventType:          ctx.EventType(),
		ClassificationType: anomType,
		LeakDetail:         ld,
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

func (c *Classifier) isBogon(prefix string, ctx *MessageContext) bool {
	// Check AS Path for Private ASNs
	if ctx.PathStr != "" {
		fields := strings.Fields(strings.Trim(ctx.PathStr, "[]"))
		for _, f := range fields {
			var asn uint32
			if _, err := fmt.Sscanf(f, "%d", &asn); err == nil {
				// Private ASNs: 64512-65534, 4200000000-4294967294
				if (asn >= 64512 && asn <= 65534) || (asn >= 4200000000 && asn <= 4294967294) {
					return true
				}
			}
		}
	}

	// Check for bogon prefixes
	pfx, err := netip.ParsePrefix(prefix)
	if err != nil {
		return false
	}

	ip := pfx.Addr()
	if ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsPrivate() {
		return true
	}

	// TEST-NET-1, TEST-NET-2, TEST-NET-3
	// 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
	if ip.Is4() {
		b := ip.As4()
		if b[0] == 192 && b[1] == 0 && b[2] == 2 {
			return true
		}
		if b[0] == 198 && b[1] == 51 && b[2] == 100 {
			return true
		}
		if b[0] == 203 && b[1] == 0 && b[2] == 113 {
			return true
		}

		// Carrier-grade NAT 100.64.0.0/10
		if b[0] == 100 && (b[1]&0b11000000) == 64 {
			return true
		}
	}

	return false
}

func (c *Classifier) detectDDoSMitigation(prefix string, ctx *MessageContext) (*LeakDetail, bool) {
	// Flowspec: look for traffic-rate in communities or specific flowspec communities
	if strings.Contains(ctx.CommStr, "traffic-rate:") || strings.Contains(ctx.CommStr, "traffic-action:") {
		return &LeakDetail{Type: DDoSFlowspec}, true
	}

	// RTBH: look for standard RTBH communities (e.g. 65535:666) or exact host routes
	pfx, err := netip.ParsePrefix(prefix)
	isHostRoute := err == nil && ((pfx.Addr().Is4() && pfx.Bits() == 32) || (pfx.Addr().Is6() && pfx.Bits() == 128))
	if strings.Contains(ctx.CommStr, "65535:666") || isHostRoute {
		return &LeakDetail{Type: DDoSRTBH}, true
	}

	// Traffic Redirection: look for scrubbing center characteristics
	// Heuristic: If we see certain route target communities often used for VRF leaking
	// or heavy AS-path prepending along with specific next-hop alterations, but
	// for a simple heuristic, we can check for common scrubbing center ASNs in the path
	// or specific community strings indicating redirection.
	// We'll rely on community hints or known scrubbing ASNs in the path.
	// Cloudflare (13335), Akamai/Prolexic (32787), Imperva (19551), Fastly (54113), Radware (30689)
	scrubbingASNs := map[uint32]bool{
		32787: true, // Prolexic
		19551: true, // Imperva
		30689: true, // Radware
	}

	if ctx.PathStr != "" {
		fields := strings.Fields(strings.Trim(ctx.PathStr, "[]"))
		for _, f := range fields {
			var asn uint32
			if _, err := fmt.Sscanf(f, "%d", &asn); err == nil {
				if scrubbingASNs[asn] {
					return &LeakDetail{Type: DDoSTrafficRedirection}, true
				}
			}
		}
	}

	return nil, false
}
