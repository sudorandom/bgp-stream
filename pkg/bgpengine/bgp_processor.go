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
	"github.com/sudorandom/bgp-stream/pkg/geoservice"
	"github.com/sudorandom/bgp-stream/pkg/utils"
	"google.golang.org/protobuf/proto"
)

type BGPEventCallback func(lat, lng float64, cc string, eventType EventType, level2Type Level2EventType, prefix string, asn uint32)
type IPCoordsProvider func(ip uint32) (float64, float64, string, geoservice.ResolutionType)
type PrefixToIPConverter func(p string) uint32

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
	recentlySeen map[uint32]struct {
		Time time.Time
		Type EventType
	}

	prefixStates map[string]*bgpproto.PrefixState

	classifier *BGPClassifier

	stateWriteQueue  chan map[string]*bgpproto.PrefixState
	stateDeleteQueue chan string

	mu       sync.Mutex
	url      string
	stopping atomic.Bool

	RecentlySeenResets atomic.Uint64
	PrefixStateResets  atomic.Uint64
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
		recentlySeen: make(map[uint32]struct {
			Time time.Time
			Type EventType
		}),
		classifier:       NewBGPClassifier(seenDB, asnMapping),
		prefixStates:     make(map[string]*bgpproto.PrefixState),
		stateWriteQueue:  make(chan map[string]*bgpproto.PrefixState, 200),
		stateDeleteQueue: make(chan string, 2000),
		url:              "wss://ris-live.ripe.net/v1/ws/?client=github.com/sudorandom/bgp-stream",
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
			now := time.Now()
			p.mu.Lock()

			for ip, entry := range pendingWithdrawals {
				if now.After(entry.Time) {
					if lat, lng, cc, resType := p.geo(ip); cc != "" {
						p.onEvent(lat, lng, cc, EventWithdrawal, Level2None, entry.Prefix, 0)
						p.recentlySeen[ip] = struct {
							Time time.Time
							Type EventType
						}{Time: now, Type: EventWithdrawal}
					} else {
						log.Printf("[GEO-DEBUG] Unknown prefix (withdrawal): %s, Resolution: %s", entry.Prefix, resType)
					}
					delete(pendingWithdrawals, ip)
				}
			}

			ticks++
			var batch map[string]*bgpproto.PrefixState
			if ticks >= 30 {
				ticks = 0
				batch = p.cleanupRecentlySeen()
			}
			p.mu.Unlock()

			if len(batch) > 0 {
				p.stateWriteQueue <- batch
			}
		}
	}()
}

func (p *BGPProcessor) cleanupRecentlySeen() map[string]*bgpproto.PrefixState {
	if len(p.recentlySeen) > 1000000 {
		p.RecentlySeenResets.Add(1)
		p.recentlySeen = make(map[uint32]struct {
			Time time.Time
			Type EventType
		})
	}

	batch := make(map[string]*bgpproto.PrefixState)
	if len(p.prefixStates) > 1000000 {
		p.PrefixStateResets.Add(1)
		p.prefixStates = make(map[string]*bgpproto.PrefixState)
	}

	return batch
}

func (p *BGPProcessor) ReportProcessorMetrics() {
	seenResets := p.RecentlySeenResets.Swap(0)
	stateResets := p.PrefixStateResets.Swap(0)

	if seenResets > 0 || stateResets > 0 {
		var sb strings.Builder
		sb.WriteString("[PROC-STATS]")
		if seenResets > 0 {
			fmt.Fprintf(&sb, " SeenResets: %d", seenResets)
		}
		if stateResets > 0 {
			if seenResets > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, " StateResets: %d", stateResets)
		}
		log.Println(sb.String())
	}
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
	now := time.Now()
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

		if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
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

	if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && last.Type == EventWithdrawal {
		p.recentlySeen[ip] = struct {
			Time time.Time
			Type EventType
		}{Time: now, Type: EventUpdate}
		return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventUpdate, level2Type: Level2None}, true
	}

	if last, ok := p.recentlySeen[ip]; ok && now.Sub(last.Time) < dedupeWindow && (last.Type == EventNew || last.Type == EventUpdate || last.Type == EventGossip) {
		return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: EventGossip, level2Type: Level2None}, true
	}

	if _, ok := pendingWithdrawals[ip]; ok {
		delete(pendingWithdrawals, ip)
		p.recentlySeen[ip] = struct {
			Time time.Time
			Type EventType
		}{Time: now, Type: EventUpdate}
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

	p.recentlySeen[ip] = struct {
		Time time.Time
		Type EventType
	}{Time: now, Type: eventType}

	return pendingEvent{ip: ip, prefix: prefix, asn: originASN, eventType: eventType, level2Type: Level2None}, true
}

func (p *BGPProcessor) classifyEvent(prefix string, ctx *MessageContext) (pendingEvent, bool) {
	if strings.Contains(prefix, ":") {
		return pendingEvent{}, false
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

	if eventType, originASN, ok := p.classifier.Evaluate(prefix, state, ctx); ok {
		return pendingEvent{
			ip:         p.prefixToIP(prefix),
			prefix:     prefix,
			asn:        originASN,
			eventType:  ctx.EventType(),
			level2Type: eventType,
		}, true
	}
	return pendingEvent{}, false
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

func (p *BGPProcessor) GetLevel2Stats() (stats map[Level2EventType]int, totalEvents int) {
	return p.classifier.GetStats()
}

func (p *BGPProcessor) SyncRPKI() error {
	if p.rpki == nil {
		return fmt.Errorf("rpki manager not initialized")
	}
	return p.rpki.Sync()
}
