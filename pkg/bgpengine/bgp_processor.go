package bgpengine

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	bgpproto "github.com/sudorandom/bgp-stream/pkg/bgpengine/proto/v1"
	"github.com/sudorandom/bgp-stream/pkg/utils"
	"google.golang.org/protobuf/proto"
)

type BGPEventCallback func(lat, lng float64, cc string, eventType EventType, level2Type Level2EventType, prefix string, asn uint32)
type IPCoordsProvider func(ip uint32) (float64, float64, string)
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
	IsWithdrawal bool
	NumPrefixes  int
	PathStr      string
	CommStr      string
	NextHop      string
	Aggregator   string
	PathLen      int
	Peer         string
	OriginASN    uint32
	Med          int32
	LocalPref    int32
	Now          time.Time
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
	Med         int32             `json:"med"`
	LocalPref   int32             `json:"local_pref"`
}

type BGPProcessor struct {
	geo          IPCoordsProvider
	seenDB       *utils.DiskTrie
	stateDB      *utils.DiskTrie
	asnMapping   *utils.ASNMapping
	onEvent      BGPEventCallback
	prefixToIP   PrefixToIPConverter
	recentlySeen map[uint32]struct {
		Time time.Time
		Type EventType
	}

	level2Stats          map[Level2EventType]int
	level2UniquePrefixes map[Level2EventType]map[string]struct{}
	totalLevel2Events    int
	prefixStates         map[string]*bgpproto.PrefixState

	mu       sync.Mutex
	url      string
	stopping atomic.Bool
}

func NewBGPProcessor(geo IPCoordsProvider, seenDB, stateDB *utils.DiskTrie, asnMapping *utils.ASNMapping, prefixToIP PrefixToIPConverter, onEvent BGPEventCallback) *BGPProcessor {
	return &BGPProcessor{
		geo:        geo,
		seenDB:     seenDB,
		stateDB:    stateDB,
		asnMapping: asnMapping,
		onEvent:    onEvent,
		prefixToIP: prefixToIP,
		recentlySeen: make(map[uint32]struct {
			Time time.Time
			Type EventType
		}),
		level2Stats:          make(map[Level2EventType]int),
		level2UniquePrefixes: make(map[Level2EventType]map[string]struct{}),
		prefixStates:         make(map[string]*bgpproto.PrefixState),
		url:                  "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream",
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
			p.handleRISMessage(&msg.Data, pendingWithdrawals)
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
			now := time.Now()
			p.mu.Lock()

			for ip, entry := range pendingWithdrawals {
				if now.After(entry.Time) {
					if lat, lng, cc := p.geo(ip); cc != "" {
						p.onEvent(lat, lng, cc, EventWithdrawal, Level2None, entry.Prefix, 0)
						p.recentlySeen[ip] = struct {
							Time time.Time
							Type EventType
						}{Time: now, Type: EventWithdrawal}
					}
					delete(pendingWithdrawals, ip)
				}
			}

			ticks++
			if ticks >= 30 {
				ticks = 0
				p.cleanupRecentlySeen(now)
			}
			p.mu.Unlock()
		}
	}()
}

func (p *BGPProcessor) cleanupRecentlySeen(now time.Time) {
	if len(p.recentlySeen) > 500000 {
		for ip, entry := range p.recentlySeen {
			if now.Sub(entry.Time) > 5*time.Minute {
				delete(p.recentlySeen, ip)
			}
		}
	}

	batch := make(map[string][]byte)
	for prefix, state := range p.prefixStates {
		if now.Sub(time.Unix(state.LastUpdateTs, 0)) > 5*time.Minute {
			if p.stateDB != nil && !p.isStopping() {
				data, err := proto.Marshal(state)
				if err == nil {
					batch[prefix] = data
				}
			}
			delete(p.prefixStates, prefix)
		}
	}

	if len(batch) > 0 && p.stateDB != nil && !p.isStopping() {
		go func(b map[string][]byte) {
			if err := p.stateDB.BatchInsertRaw(b); err != nil {
				log.Printf("Error saving prefix states: %v", err)
			}
		}(batch)
	}
}

func (p *BGPProcessor) handleRISMessage(data *RISMessageData, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
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

	now := time.Now()
	p.handleWithdrawals(data.Withdrawals, originASN, now, pendingWithdrawals)
	p.handleAnnouncements(data.Announcements, originASN, now, pendingWithdrawals)

	ctx := &MessageContext{
		Peer:       data.Peer,
		Aggregator: data.Aggregator,
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
			p.classifyEvent(prefix, ctx)
		}
	}

	ctx.IsWithdrawal = false
	for _, ann := range data.Announcements {
		ctx.NumPrefixes = len(ann.Prefixes)
		ctx.NextHop = ann.NextHop
		for _, prefix := range ann.Prefixes {
			p.classifyEvent(prefix, ctx)
		}
	}
}

func (p *BGPProcessor) handleWithdrawals(withdrawals []string, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	for _, prefix := range withdrawals {
		ip := p.prefixToIP(prefix)
		if ip == 0 {
			continue
		}

		if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
			if lat, lng, cc := p.geo(ip); cc != "" {
				p.onEvent(lat, lng, cc, EventGossip, Level2None, prefix, originASN)
			}
			continue
		}

		pendingWithdrawals[ip] = struct {
			Time   time.Time
			Prefix string
		}{Time: now.Add(withdrawResolutionWindow), Prefix: prefix}
	}
}

func (p *BGPProcessor) handleAnnouncements(announcements []struct {
	NextHop  string   `json:"next_hop"`
	Prefixes []string `json:"prefixes"`
}, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	for _, ann := range announcements {
		for _, prefix := range ann.Prefixes {
			p.processAnnouncement(prefix, originASN, now, pendingWithdrawals)
		}
	}
}

func (p *BGPProcessor) processAnnouncement(prefix string, originASN uint32, now time.Time, pendingWithdrawals map[uint32]struct {
	Time   time.Time
	Prefix string
}) {
	ip := p.prefixToIP(prefix)
	if ip == 0 {
		return
	}

	if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventUpdate, Level2None, prefix, originASN)
			p.recentlySeen[ip] = struct {
				Time time.Time
				Type EventType
			}{Time: now, Type: EventUpdate}
		}
		return
	}

	if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && (last.Type == EventNew || last.Type == EventUpdate || last.Type == EventGossip) {
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventGossip, Level2None, prefix, originASN)
		}
		return
	}

	if _, ok := pendingWithdrawals[ip]; ok {
		delete(pendingWithdrawals, ip)
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventUpdate, Level2None, prefix, originASN)
			p.recentlySeen[ip] = struct {
				Time time.Time
				Type EventType
			}{Time: now, Type: EventUpdate}
		}
	} else {
		p.handleNewOrUpdate(prefix, ip, originASN, now)
	}
}

func (p *BGPProcessor) handleNewOrUpdate(prefix string, ip, originASN uint32, now time.Time) {
	isNew := true
	if p.seenDB != nil {
		if val, _ := p.seenDB.Get(prefix); val != nil {
			isNew = false
		}
	}

	if isNew {
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventNew, Level2Discovery, prefix, originASN)
			p.recentlySeen[ip] = struct {
				Time time.Time
				Type EventType
			}{Time: now, Type: EventNew}
		}
	} else {
		if lat, lng, cc := p.geo(ip); cc != "" {
			p.onEvent(lat, lng, cc, EventUpdate, Level2PolicyChurn, prefix, originASN)
			p.recentlySeen[ip] = struct {
				Time time.Time
				Type EventType
			}{Time: now, Type: EventUpdate}
		}
	}
}

func (p *BGPProcessor) classifyEvent(prefix string, ctx *MessageContext) {
	if strings.Contains(prefix, ":") {
		return
	}
	state, ok := p.prefixStates[prefix]
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
		p.prefixStates[prefix] = state
	}

	state.LastUpdateTs = ctx.Now.Unix()
	bucket := p.getOrCreateBucket(state, ctx.Now)
	bucket.TotalMessages++

	if ctx.IsWithdrawal {
		bucket.Withdrawals++
	} else {
		p.updateAnnouncementStats(state, bucket, ctx)
	}

	// If already classified, emit the classification pulse immediately for this peer
	if state.ClassifiedType != 0 {
		if ctx.Now.Unix()-state.ClassifiedTimeTs > 600 {
			state.ClassifiedType = 0
			state.ClassifiedTimeTs = 0
			state.UncategorizedCounted = false
		} else {
			if lat, lng, cc := p.geo(p.prefixToIP(prefix)); cc != "" {
				p.onEvent(lat, lng, cc, ctx.EventType(), Level2EventType(state.ClassifiedType), prefix, ctx.OriginASN)
			}
			return
		}
	}

	p.evaluatePrefixState(prefix, state, ctx)
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
}

func (p *BGPProcessor) evaluatePrefixState(prefix string, state *bgpproto.PrefixState, ctx *MessageContext) {
	stats := p.aggregateRecentBuckets(state, ctx.Now)

	elapsed := float64(ctx.Now.Unix() - stats.earliestTS)
	if elapsed < 60 {
		elapsed = float64(ctx.Now.Unix() - state.StartTimeTs)
	}
	if elapsed <= 0 {
		elapsed = 1
	}

	// Ensure we have seen enough messages over a small time window to classify
	if elapsed < 120 && stats.totalMsgs < 5 {
		return
	}

	eventType, classified := p.findClassification(prefix, state, stats, elapsed, ctx)

	if classified {
		p.recordClassification(prefix, state, eventType, ctx.Now.Unix())
	} else if stats.totalMsgs > 50 && !state.UncategorizedCounted {
		// If it has significant messages but hasn't matched a rule, count as Discovery
		p.recordClassification(prefix, state, Level2Discovery, ctx.Now.Unix())
		state.UncategorizedCounted = true
	}
}

func (p *BGPProcessor) aggregateRecentBuckets(state *bgpproto.PrefixState, now time.Time) prefixStats {
	for peer, attr := range state.PeerLastAttrs {
		if now.Unix()-attr.LastUpdateTs > 3600 {
			delete(state.PeerLastAttrs, peer)
		}
	}

	cutoff := now.Add(-5 * time.Minute).Unix()
	s := prefixStats{
		earliestTS: now.Unix(),
		uniqueHops: make(map[string]bool),
		uniqueASNs: make(map[uint32]bool),
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

	for _, attr := range state.PeerLastAttrs {
		if attr.NextHop != "" {
			s.uniqueHops[attr.NextHop] = true
		}
		if attr.OriginAsn != 0 {
			s.uniqueASNs[attr.OriginAsn] = true
		}
	}
	return s
}

func (p *BGPProcessor) findClassification(prefix string, state *bgpproto.PrefixState, s prefixStats, elapsed float64, ctx *MessageContext) (Level2EventType, bool) {
	numPeers := float64(len(state.PeerLastAttrs))
	if numPeers == 0 {
		numPeers = 1
	}
	perPeerRate := float64(s.totalMsgs) / numPeers

	// 1. Critical
	if et, ok := p.findCriticalAnomaly(prefix, state, s, ctx); ok {
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

func (p *BGPProcessor) findCriticalAnomaly(prefix string, state *bgpproto.PrefixState, s prefixStats, ctx *MessageContext) (Level2EventType, bool) {
	if s.totalWith >= 3 && s.totalAnn == 0 {
		return Level2Outage, true
	}
	if p.hasRouteLeak(prefix, ctx) {
		return Level2RouteLeak, true
	}
	return Level2None, false
}

func (p *BGPProcessor) findBadAnomaly(s prefixStats, elapsed, perPeerRate float64) (Level2EventType, bool) {
	if s.totalAgg > 10 && float64(s.totalAgg)/elapsed > 0.05 {
		return Level2AggFlap, true
	}
	if len(s.uniqueHops) > 1 && s.totalHop >= 5 && s.totalPath <= 1 {
		return Level2NextHopOscillation, true
	}
	if perPeerRate > 5.0 && s.totalMsgs > 10 || (s.totalMsgs > 15 && s.totalPath == 0 && s.totalComm == 0 && s.totalMed == 0 && s.totalLP == 0) {
		return Level2Babbling, true
	}
	if s.totalWith > 5 && float64(s.totalAnn)/float64(s.totalWith) < 2.5 {
		return Level2LinkFlap, true
	}
	return Level2None, false
}

func (p *BGPProcessor) findNormalAnomaly(s prefixStats, elapsed float64) (Level2EventType, bool) {
	if s.totalAnn >= 3 && s.totalIncreases >= 2 && s.totalDecreases == 0 && s.totalWith >= 1 {
		return Level2PathHunting, true
	}
	if s.totalComm >= 5 || (s.totalPath >= 5 && s.totalIncreases+s.totalDecreases <= 1) || (s.totalMed+s.totalLP >= 3 && s.totalPath <= 2) {
		return Level2PolicyChurn, true
	}
	if s.totalAnn > 15 && s.totalPath <= 5 && s.totalWith <= 2 {
		return Level2Discovery, true
	}
	if (s.totalIncreases+s.totalDecreases) >= 3 && float64(s.totalIncreases+s.totalDecreases)/elapsed > 0.01 {
		return Level2PathLengthOscillation, true
	}
	return Level2None, false
}

func (p *BGPProcessor) recordClassification(prefix string, state *bgpproto.PrefixState, eventType Level2EventType, now int64) {
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

	// Trigger visual event for classification
	if lat, lng, cc := p.geo(p.prefixToIP(prefix)); cc != "" {
		p.onEvent(lat, lng, cc, EventUpdate, eventType, prefix, originASN)
	}

	// Record that this prefix is now classified
	state.ClassifiedType = int32(eventType)
	state.ClassifiedTimeTs = now

	if p.stateDB != nil {
		go p.deleteState(prefix)
	}
}

func (p *BGPProcessor) deleteState(prefix string) {
	if p.stateDB == nil || p.isStopping() {
		return
	}
	if err := p.stateDB.DeleteRaw([]byte(prefix)); err != nil {
		log.Printf("Error deleting prefix state: %v", err)
	}
}

func (p *BGPProcessor) hasRouteLeak(prefix string, ctx *MessageContext) bool {
	if ctx.PathStr == "" {
		return false
	}
	// Basic path parser (path is stored as string "[1 2 3]")
	fields := strings.Fields(strings.Trim(ctx.PathStr, "[]"))
	if len(fields) < 3 {
		return false
	}

	path := make([]uint32, 0, len(fields))
	for _, f := range fields {
		var asn uint32
		if _, err := fmt.Sscanf(f, "%d", &asn); err == nil {
			path = append(path, asn)
		}
	}

	// Valley-free violation: Tier-1 -> Non-Tier-1/Non-Cloud -> Tier-1
	for i := 0; i < len(path)-2; i++ {
		if p.isTier1(path[i]) && !p.isTier1(path[i+1]) && !p.isCloud(path[i+1]) && p.isTier1(path[i+2]) {
			p.logRouteLeak(prefix, path)
			return true
		}
	}

	return false
}

func (p *BGPProcessor) logRouteLeak(prefix string, path []uint32) {
	var pathStrs []string
	for _, asn := range path {
		name := "Unknown"
		if p.asnMapping != nil {
			name = p.asnMapping.GetName(asn)
		}
		pathStrs = append(pathStrs, fmt.Sprintf("AS%d (%s)", asn, name))
	}
	log.Printf("[!!! ROUTE LEAK !!!] Prefix: %s, Path: %s", prefix, strings.Join(pathStrs, " -> "))
}

func (p *BGPProcessor) isTier1(asn uint32) bool {
	switch asn {
	case 209, 701, 1239, 1299, 2828, 2914, 3257, 3320, 3356, 3491, 3549, 3561, 5511, 6453, 6461, 6762, 6830, 7018, 12956: // global Tier-1s
		return true
	case 4134, 4809, 4837, 7473, 174, 6939, 9002, 1273, 4637, 7922: // regional Tier-1s
		return true
	default:
		return false
	}
}

func (p *BGPProcessor) isCloud(asn uint32) bool {
	switch asn {
	case 13335, 15169, 16509, 14618, 20940, 8075, 32934, 31898, 40027, 36040: // Major cloud providers
		return true
	default:
		return false
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
