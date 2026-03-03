package bgpengine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgpengine/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
	"google.golang.org/protobuf/proto"
)

type BGPEventCallback func(lat, lng float64, cc string, eventType EventType, level2Type Level2EventType, prefix string, asn uint32)
type IPCoordsProvider func(ip uint32) (float64, float64, string, geoservice.ResolutionType)
type PrefixToIPConverter func(p string) uint32

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

type RISMessageData struct {
	Announcements []struct {
		NextHop  string   `json:"next_hop"`
		Prefixes []string `json:"prefixes"`
	} `json:"announcements"`
	Withdrawals []string          `json:"withdrawals"`
	Path        []json.RawMessage `json:"path"`
	Community   [][]interface{}   `json:"community"`
	Aggregator  string            `json:"aggregator"`
	Peer        string            `json:"peer"`
	Host        string            `json:"host"`
	Med         int32             `json:"med"`
	LocalPref   int32             `json:"local_pref"`
}

type BGPProcessor struct {
	geo          IPCoordsProvider
	seenDB       *utils.DiskTrie
	stateDB      *utils.DiskTrie
	asnMapping   *utils.ASNMapping
	rpki         *utils.RPKIManager
	onEvent      BGPEventCallback
	prefixToIP   PrefixToIPConverter
	recentlySeen *utils.LRUCache[uint32, struct {
		Time time.Time
		Type EventType
	}]

	level2Stats          map[Level2EventType]int
	level2UniquePrefixes map[Level2EventType]map[string]struct{}
	totalLevel2Events    int
	prefixStates         *utils.LRUCache[string, *bgpproto.PrefixState]

	stateWriteQueue  chan map[string]*bgpproto.PrefixState
	stateDeleteQueue chan string

	mu       sync.Mutex
	url      string
	stopping atomic.Bool
}

func NewBGPProcessor(geo IPCoordsProvider, seenDB, stateDB *utils.DiskTrie, asnMapping *utils.ASNMapping, rpki *utils.RPKIManager, prefixToIP PrefixToIPConverter, onEvent BGPEventCallback) *BGPProcessor {
	p := &BGPProcessor{
		geo:        geo,
		seenDB:     seenDB,
		stateDB:    stateDB,
		asnMapping: asnMapping,
		rpki:       rpki,
		onEvent:    onEvent,
		prefixToIP: prefixToIP,
		recentlySeen: utils.NewLRUCache[uint32, struct {
			Time time.Time
			Type EventType
		}](1000000),
		level2Stats:          make(map[Level2EventType]int),
		level2UniquePrefixes: make(map[Level2EventType]map[string]struct{}),
		prefixStates:         utils.NewLRUCache[string, *bgpproto.PrefixState](1000000),
		stateWriteQueue:      make(chan map[string]*bgpproto.PrefixState, 200),
		stateDeleteQueue:     make(chan string, 2000),
		url:                  "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream",
	}

	if stateDB != nil {
		go p.stateWorker()
	}

	return p
}

func (p *BGPProcessor) stateWorker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		if p.isStopping() {
			return
		}
		select {
		case batch := <-p.stateWriteQueue:
			rawBatch := make(map[string][]byte)
			for prefix, state := range batch {
				data, err := proto.Marshal(state)
				if err == nil {
					rawBatch[prefix] = data
				}
			}
			if len(rawBatch) > 0 {
				if err := p.stateDB.BatchInsertRaw(rawBatch); err != nil {
					log.Printf("Error saving prefix states: %v", err)
				}
			}
		case prefix := <-p.stateDeleteQueue:
			if err := p.stateDB.DeleteRaw([]byte(prefix)); err != nil {
				log.Printf("Error deleting prefix state: %v", err)
			}
		case <-ticker.C:
			// periodically check stopping
		}
	}
}

func (p *BGPProcessor) Close() {
	p.stopping.Store(true)
}

func (p *BGPProcessor) isStopping() bool {
	return p.stopping.Load()
}

const dedupeWindow = 15 * time.Second
const withdrawResolutionWindow = 10 * time.Second

func (p *BGPProcessor) Listen() {
	pendingWithdrawals := make(map[uint32]struct {
		Time   time.Time
		Prefix string
	})

	p.startWithdrawalPacer(pendingWithdrawals)

	backoff := 1 * time.Second
	for {
		c, err := p.connectAndSubscribe()
		if err != nil {
			log.Printf("Connection error: %v. Retrying in %v...", err, backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			continue
		}
		backoff = 1 * time.Second

		p.runMessageLoop(c, pendingWithdrawals)
		_ = c.Close()
		time.Sleep(time.Second)
	}
}

func (p *BGPProcessor) connectAndSubscribe() (*websocket.Conn, error) {
	log.Printf("Connecting to RIS Live: %s", p.url)
	c, resp, err := websocket.DefaultDialer.Dial(p.url, nil)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}

	subscribeMsg := `{"type": "ris_subscribe", "data": {"type": "UPDATE", "prefix": "0.0.0.0/0", "moreSpecific": true}}`
	if err := c.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

type pendingEvent struct {
	ip         uint32
	prefix     string
	asn        uint32
	eventType  EventType
	level2Type Level2EventType
}

func (p *BGPProcessor) runMessageLoop(c *websocket.Conn, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	for {
		if p.isStopping() {
			return
		}
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Printf("Read error: %v. Reconnecting...", err)
			return
		}
		var msg struct {
			Type string         `json:"type"`
			Data RISMessageData `json:"data"`
		}
		if json.Unmarshal(message, &msg) != nil {
			continue
		}

		if msg.Type == "ris_error" {
			log.Printf("[RIS ERROR] %s", string(message))
			continue
		}
		if msg.Type == "ris_message" {
			events := p.handleRISMessage(&msg.Data, pendingWithdrawals)
			for _, e := range events {
				if lat, lng, cc, _ := p.geo(e.ip); cc != "" {
					p.onEvent(lat, lng, cc, e.eventType, e.level2Type, e.prefix, e.asn)
				}
			}
		}
	}
}

func (p *BGPProcessor) startWithdrawalPacer(pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		ticks := 0
		for range ticker.C {
			now := Now()
			p.mu.Lock()

			for ip, entry := range pendingWithdrawals {
				if now.After(entry.Time) {
					if lat, lng, cc, resType := p.geo(ip); cc != "" {
						p.onEvent(lat, lng, cc, EventWithdrawal, Level2None, entry.Prefix, 0)
						p.recentlySeen.Add(ip, struct {
							Time time.Time
							Type EventType
						}{Time: now, Type: EventWithdrawal})
					} else {
						log.Printf("[GEO-DEBUG] Unknown prefix (withdrawal): %s, Resolution: %s", entry.Prefix, resType)
					}
					delete(pendingWithdrawals, ip)
				}
			}

			ticks++
			p.mu.Unlock()
		}
	}()
}

func (p *BGPProcessor) ReportProcessorMetrics() {
}

func (p *BGPProcessor) handleRISMessage(data *RISMessageData, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) []pendingEvent {
	p.mu.Lock()
	defer p.mu.Unlock()

	var originASN uint32
	if len(data.Path) > 0 {
		last := data.Path[len(data.Path)-1]
		var asn uint32
		if err := json.Unmarshal(last, &asn); err == nil {
			originASN = asn
		}
	}

	var events []pendingEvent
	now := Now()
	events = append(events, p.handleWithdrawals(data.Withdrawals, originASN, now, pendingWithdrawals)...)
	events = append(events, p.handleAnnouncements(data.Announcements, originASN, now, pendingWithdrawals)...)

	ctx := &MessageContext{
		Peer:       data.Peer,
		Aggregator: data.Aggregator,
		Host:       data.Host,
		OriginASN:  originASN,
		Med:        data.Med,
		LocalPref:  data.LocalPref,
		Now:        now,
	}
	if len(data.Path) > 0 {
		ctx.PathLen = len(data.Path)
		pathParts := make([]string, len(data.Path))
		for i, p := range data.Path {
			pathParts[i] = string(p)
		}
		ctx.PathStr = "[" + strings.Join(pathParts, " ") + "]"
	}
	if len(data.Community) > 0 {
		ctx.CommStr = fmt.Sprintf("%v", data.Community)
	}

	if len(data.Withdrawals) > 0 {
		ctx.IsWithdrawal = true
		ctx.NumPrefixes = len(data.Withdrawals)
		for _, prefix := range data.Withdrawals {
			if e, ok := p.classifyEvent(prefix, ctx); ok {
				events = append(events, e)
			}
		}
	}

	ctx.IsWithdrawal = false
	for _, ann := range data.Announcements {
		ctx.NumPrefixes = len(ann.Prefixes)
		ctx.NextHop = ann.NextHop
		for _, prefix := range ann.Prefixes {
			if e, ok := p.classifyEvent(prefix, ctx); ok {
				events = append(events, e)
			}
		}
	}
	return events
}

func (p *BGPProcessor) handleWithdrawals(withdrawals []string, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) []pendingEvent {
	var events []pendingEvent
	for _, prefix := range withdrawals {
		ip := p.prefixToIP(prefix)
		if ip == 0 {
			continue
		}

		if last, ok := p.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
			events = append(events, pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventGossip, level2Type: Level2None})
			continue
		}

		pendingWithdrawals[ip] = struct {
			Time   time.Time
			Prefix string
		}{Time: now.Add(withdrawResolutionWindow), Prefix: prefix}
	}
	return events
}

func (p *BGPProcessor) handleAnnouncements(announcements []struct {
	NextHop  string   `json:"next_hop"`
	Prefixes []string `json:"prefixes"`
}, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) []pendingEvent {
	var events []pendingEvent
	for _, ann := range announcements {
		for _, prefix := range ann.Prefixes {
			if e, ok := p.processAnnouncement(prefix, originASN, now, pendingWithdrawals); ok {
				events = append(events, e)
			}
		}
	}
	return events
}

func (p *BGPProcessor) processAnnouncement(prefix string, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) (pendingEvent, bool) {
	ip := p.prefixToIP(prefix)
	if ip == 0 {
		return pendingEvent{}, false
	}

	if last, ok := p.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
		p.recentlySeen.Add(ip, struct {
			Time time.Time
			Type EventType
		}{Time: now, Type: EventUpdate})
		return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventUpdate, level2Type: Level2None}, true
	}

	if last, ok := p.recentlySeen.Get(ip); ok && now.Sub(last.Time) < dedupeWindow && (last.Type == EventNew || last.Type == EventUpdate || last.Type == EventGossip) {
		return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventGossip, level2Type: Level2None}, true
	}

	if _, ok := pendingWithdrawals[ip]; ok {
		delete(pendingWithdrawals, ip)
		p.recentlySeen.Add(ip, struct {
			Time time.Time
			Type EventType
		}{Time: now, Type: EventUpdate})
		return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventUpdate, level2Type: Level2None}, true
	}

	return p.handleNewOrUpdate(prefix, ip, originASN, now)
}

func (p *BGPProcessor) handleNewOrUpdate(prefix string, ip, originASN uint32, now time.Time) (pendingEvent, bool) {
	isNew := true
	if p.seenDB != nil {
		if val, _ := p.seenDB.Get(prefix); val != nil {
			isNew = false
		}
	}

	eventType := EventUpdate
	if isNew {
		eventType = EventNew
	}

	p.recentlySeen.Add(ip, struct {
		Time time.Time
		Type EventType
	}{Time: now, Type: eventType})

	return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: eventType, level2Type: Level2None}, true
}

func (p *BGPProcessor) classifyEvent(prefix string, ctx *MessageContext) (pendingEvent, bool) {
	if strings.Contains(prefix, ":") {
		return pendingEvent{}, false
	}
	state, ok := p.prefixStates.Get(prefix)
	if !ok {
		// Try to load from stateDB
		if p.stateDB != nil {
			data, err := p.stateDB.Get(prefix)
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
		p.prefixStates.Add(prefix, state)
	}

	state.LastUpdateTs = ctx.Now.Unix()
	bucket := p.getOrCreateBucket(state, ctx.Now)
	bucket.TotalMessages++

	if ctx.IsWithdrawal {
		bucket.Withdrawals++
	} else {
		p.updateAnnouncementStats(state, bucket, ctx)
		if p.rpki != nil && ctx.OriginASN != 0 {
			status, err := p.rpki.Validate(prefix, ctx.OriginASN)
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
		} else if Level2EventType(state.ClassifiedType) != Level2Discovery &&
			Level2EventType(state.ClassifiedType) != Level2PolicyChurn &&
			Level2EventType(state.ClassifiedType) != Level2PathHunting &&
			Level2EventType(state.ClassifiedType) != Level2PathLengthOscillation {
			// If it's a Normal anomaly, we still allow upgrade to Bad/Critical
			return pendingEvent{
				ip:         p.prefixToIP(prefix),
				prefix:     prefix,
				asn:        ctx.OriginASN,
				eventType:  ctx.EventType(),
				level2Type: Level2EventType(state.ClassifiedType),
			}, true
		}
	}

	return p.evaluatePrefixState(prefix, state, ctx)
}

func (p *BGPProcessor) getOrCreateBucket(state *bgpproto.PrefixState, now time.Time) *bgpproto.StatsBucket {
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

func (p *BGPProcessor) updateAnnouncementStats(state *bgpproto.PrefixState, bucket *bgpproto.StatsBucket, ctx *MessageContext) {
	bucket.Announcements++

	peer := ctx.Peer
	if state.PeerLastAttrs == nil {
		state.PeerLastAttrs = make(map[string]*bgpproto.LastAttrs)
	}

	if last, ok := state.PeerLastAttrs[peer]; ok {
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

func (p *BGPProcessor) evaluatePrefixState(prefix string, state *bgpproto.PrefixState, ctx *MessageContext) (pendingEvent, bool) {
	stats := p.aggregateRecentBuckets(state, ctx.Now, ctx.OriginASN)

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

	eventType, classified := p.findClassification(prefix, state, &stats, elapsed, ctx)

	if classified {
		if state.ClassifiedType != 0 {
			if p.getPriority(eventType) <= p.getPriority(Level2EventType(state.ClassifiedType)) {
				// We already have a classification of equal or higher priority
				// Just return the event for the current classification
				return pendingEvent{
					ip:         p.prefixToIP(prefix),
					prefix:     prefix,
					asn:        ctx.OriginASN,
					eventType:  ctx.EventType(),
					level2Type: Level2EventType(state.ClassifiedType),
				}, true
			}
		}
		return p.recordClassification(prefix, state, eventType, ctx.Now.Unix()), true
	}
	return pendingEvent{}, false
}

func (p *BGPProcessor) getPriority(t Level2EventType) int {
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

func (p *BGPProcessor) aggregateRecentBuckets(state *bgpproto.PrefixState, now time.Time, currentOriginASN uint32) prefixStats {
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

func (p *BGPProcessor) findClassification(prefix string, state *bgpproto.PrefixState, s *prefixStats, elapsed float64, ctx *MessageContext) (Level2EventType, bool) {
	numPeers := float64(len(state.PeerLastAttrs))
	if numPeers == 0 {
		numPeers = 1
	}
	perPeerRate := float64(s.totalMsgs) / numPeers

	// 1. Critical
	if et, ok := p.findCriticalAnomaly(prefix, s, elapsed, ctx); ok {
		return et, true
	}

	// 2. Bad
	if et, ok := p.findBadAnomaly(s, elapsed, perPeerRate); ok {
		return et, true
	}

	// 3. Normal / Policy
	if et, ok := p.findNormalAnomaly(s, elapsed); ok {
		return et, true
	}

	return Level2None, false
}

func (p *BGPProcessor) getHistoricalASN(prefix string) uint32 {
	if p.seenDB == nil {
		return 0
	}
	val, _ := p.seenDB.Get(prefix)
	if len(val) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(val)
}

func (p *BGPProcessor) findCriticalAnomaly(prefix string, s *prefixStats, elapsed float64, ctx *MessageContext) (Level2EventType, bool) {
	if s.totalWith >= 3 && s.totalAnn == 0 && elapsed > 60 {
		return Level2Outage, true
	}

	peerCount := len(s.uniquePeers)
	hostCount := len(s.uniqueHosts)

	// 1. RPKI Validation Check (The Signal)
	if ctx.OriginASN != 0 && utils.RPKIStatus(ctx.LastRpkiStatus) == utils.RPKIInvalidASN {
		isDDoS := p.isDDoSProvider(ctx.OriginASN)

		// Historical Origin Check (The Filter)
		historicalASN := p.getHistoricalASN(prefix)
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
	if segment, ok := p.hasRouteLeak(ctx); ok {
		// Consensus requirement for path violations to filter out terminal edge/collector leaks.
		// If we see a Tier-1 -> Stub -> Tier-1 path from multiple vantage points,
		// it is highly likely to be a real traffic-impacting leak.
		if peerCount >= 3 && hostCount >= 2 {
			p.logRouteLeak(prefix, segment)
			return Level2RouteLeak, true
		}
	}

	return Level2None, false
}

func (p *BGPProcessor) isDDoSProvider(asn uint32) bool {
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

func (p *BGPProcessor) hasRouteLeak(ctx *MessageContext) ([3]uint32, bool) {
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
	if p.asnMapping != nil {
		lastOrgID = p.asnMapping.GetOrgID(rawPath[0])
	}

	for i := 1; i < len(rawPath); i++ {
		currentASN := rawPath[i]
		currentOrgID := ""
		if p.asnMapping != nil {
			currentOrgID = p.asnMapping.GetOrgID(currentASN)
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
		if p.isTier1(path[i]) && !p.isTier1(path[i+1]) && !p.isCloud(path[i+1]) && p.isTier1(path[i+2]) {
			return [3]uint32{path[i], path[i+1], path[i+2]}, true
		}
	}

	return [3]uint32{}, false
}

func (p *BGPProcessor) logRouteLeak(prefix string, segment [3]uint32) {
	pathStrs := make([]string, 3)
	for i, asn := range segment {
		name := "Unknown"
		if p.asnMapping != nil {
			name = p.asnMapping.GetName(asn)
		}
		pathStrs[i] = fmt.Sprintf("AS%d (%s)", asn, name)
	}
	log.Printf("[!!! ROUTE LEAK !!!] Prefix: %s, Path Segment: %s", prefix, strings.Join(pathStrs, " -> "))
}

func (p *BGPProcessor) isTier1(asn uint32) bool {
	switch asn {
	case 209, 701, 702, 1239, 1299, 2828, 2914, 3257, 3320, 3356, 3491, 3549, 3561, 5511, 6453, 6461, 6762, 6830, 7018, 12956: // Global Tier-1s
		return true
	case 174, 6939, 9002, 1273, 4637, 7922, 4134, 4809, 4837, 7473, 9808: // Major Regional/National Backbones
		return true
	default:
		return false
	}
}

func (p *BGPProcessor) isCloud(asn uint32) bool {
	switch asn {
	case 13335, 15169, 16509, 14618, 20940, 8075, 32934, 31898, 40027, 36040, 54113, 14061, 37963, 45102, 16625: // Major Cloud/CDN
		return true
	default:
		return false
	}
}

func (p *BGPProcessor) findBadAnomaly(s *prefixStats, elapsed, perPeerRate float64) (Level2EventType, bool) {
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

func (p *BGPProcessor) findNormalAnomaly(s *prefixStats, elapsed float64) (Level2EventType, bool) {
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

func (p *BGPProcessor) recordClassification(prefix string, state *bgpproto.PrefixState, eventType Level2EventType, now int64) pendingEvent {
	p.level2Stats[eventType]++
	if p.level2UniquePrefixes[eventType] == nil {
		p.level2UniquePrefixes[eventType] = make(map[string]struct{})
	}
	p.level2UniquePrefixes[eventType][prefix] = struct{}{}
	p.totalLevel2Events++

	// Get an origin ASN for visualization
	originASN := uint32(0)
	for _, attr := range state.PeerLastAttrs {
		if attr.OriginAsn != 0 {
			originASN = attr.OriginAsn
			break
		}
	}

	// Record that this prefix is now classified
	state.ClassifiedType = int32(eventType)
	state.ClassifiedTimeTs = now

	return pendingEvent{
		ip:         p.prefixToIP(prefix),
		prefix:     prefix,
		asn:        originASN,
		eventType:  EventUpdate,
		level2Type: eventType,
	}
}

func (p *BGPProcessor) GetLevel2Stats() (stats map[Level2EventType]int, totalEvents int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	statsCopy := make(map[Level2EventType]int)
	for k, v := range p.level2Stats {
		statsCopy[k] = v
	}

	return statsCopy, p.totalLevel2Events
}

func (p *BGPProcessor) SyncRPKI() error {
	if p.rpki == nil {
		return fmt.Errorf("rpki manager not initialized")
	}
	return p.rpki.Sync()
}
