package bgpengine

import (
	"fmt"
	"image/color"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/biter777/countries"
	"github.com/hajimehoshi/ebiten/v2/text/v2"
	"github.com/sudorandom/bgp-stream/pkg/bgp"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

type hub struct {
	cc   string
	rate float64
}

type windowBucket struct {
	countryActivity map[string]int
	anomalyActivity map[bgp.ClassificationType]map[string]int
}

func getMaskLen(prefix string) int {
	idx := strings.IndexByte(prefix, '/')
	if idx == -1 {
		return 0
	}
	mask, _ := strconv.Atoi(prefix[idx+1:])
	return mask
}

type statsEvent struct {
	ev         *bgpEvent
	name       string
	c          color.RGBA
	uiInterval float64
	trigger    bool
}

type prefixClassState struct {
	ct bgp.ClassificationType
	ts time.Time
}

type statsWorkerState struct {
	prefixToASN            map[string]uint32
	prefixToClassification map[string]prefixClassState
	visualImpact           map[string]*VisualImpact

	impactMap       map[string]*VisualImpact
	countMap        map[string]*PrefixCount
	asnsPerClass    map[string]map[uint32]struct{}
	asnGroups       map[asnGroupKey]*asnGroup
	asnSortedGroups []*asnGroup
	hubCurrent      []hub

	// Rolling window (60 seconds)
	buckets    [60]windowBucket
	currentIdx int
}

func (e *Engine) runStatsWorker() {
	state := &statsWorkerState{
		prefixToASN:            make(map[string]uint32),
		prefixToClassification: make(map[string]prefixClassState),
		visualImpact:           make(map[string]*VisualImpact),
		impactMap:              make(map[string]*VisualImpact),
		countMap:               make(map[string]*PrefixCount),
		asnsPerClass:           make(map[string]map[uint32]struct{}),
		asnGroups:              make(map[asnGroupKey]*asnGroup),
		asnSortedGroups:        make([]*asnGroup, 0, 100),
		hubCurrent:             make([]hub, 0, 300),
	}
	for i := range state.buckets {
		state.buckets[i].countryActivity = make(map[string]int)
		state.buckets[i].anomalyActivity = make(map[bgp.ClassificationType]map[string]int)
	}

	for msg := range e.statsCh {
		if msg.trigger {
			e.processStatsTrigger(state)
			continue
		}
		e.processStatsEvent(msg, state)
	}
}

func (e *Engine) processStatsTrigger(state *statsWorkerState) {
	// 1. Calculate stats over the rolling 60s window
	activeHubs := e.calculateActiveHubs(state)
	allImpact := e.gatherActiveImpactsWorker(state)
	prefixCounts := e.calculatePrefixCounts(state, allImpact)
	activeASNImpacts := e.calculateASNImpacts(state, allImpact)

	// 2. Update engine state
	e.metricsMu.Lock()
	for _, vh := range e.VisualHubs {
		vh.Active = false
		vh.TargetAlpha = 0.0
	}
	e.ActiveHubs = e.ActiveHubs[:0]
	for _, newHub := range activeHubs {
		existing, ok := e.VisualHubs[newHub.CC]
		if !ok {
			existing = newHub
			e.VisualHubs[newHub.CC] = existing
		} else {
			existing.Active = true
			existing.TargetY = newHub.TargetY
			if existing.Alpha < 0.01 {
				existing.DisplayY = newHub.TargetY
			}
			existing.TargetAlpha = 1.0
			existing.Rate = newHub.Rate
			existing.RateStr = newHub.RateStr
			existing.RateWidth = newHub.RateWidth
		}
		e.ActiveHubs = append(e.ActiveHubs, existing)
	}
	for cc, vh := range e.VisualHubs {
		if !vh.Active {
			delete(e.VisualHubs, cc)
		}
	}

	e.prefixCounts = prefixCounts
	e.ActiveASNImpacts = activeASNImpacts
	e.metricsMu.Unlock()

	// 3. Advance the rolling window
	state.currentIdx = (state.currentIdx + 1) % 60
	// Clear the next bucket
	clear(state.buckets[state.currentIdx].countryActivity)
	for ct := range state.buckets[state.currentIdx].anomalyActivity {
		clear(state.buckets[state.currentIdx].anomalyActivity[ct])
	}
}

func (e *Engine) calculateActiveHubs(state *statsWorkerState) []*VisualHub {
	// Aggregate country activity over 60s
	aggregated := make(map[string]int)
	for i := 0; i < 60; i++ {
		for cc, val := range state.buckets[i].countryActivity {
			aggregated[cc] += val
		}
	}

	state.hubCurrent = state.hubCurrent[:0]
	for cc, val := range aggregated {
		// Rate is messages per second over 60s
		state.hubCurrent = append(state.hubCurrent, hub{cc, float64(val) / 60.0})
	}
	sort.Slice(state.hubCurrent, func(i, j int) bool { return state.hubCurrent[i].rate > state.hubCurrent[j].rate })

	maxItems := 5
	if len(state.hubCurrent) < maxItems {
		maxItems = len(state.hubCurrent)
	}

	activeHubs := make([]*VisualHub, 0, maxItems)
	hubYBase := float64(e.Height) * 0.48
	fontSize := 18.0
	if e.Width > 2000 {
		fontSize = 36.0
		hubYBase = float64(e.Height) * 0.42
	}
	spacing := fontSize * 1.0

	for i := 0; i < maxItems; i++ {
		targetY := hubYBase + float64(i)*spacing

		cc := state.hubCurrent[i].cc
		countryName := countries.ByName(cc).String()
		if countryName == bgp.StrUnknown {
			countryName = cc
		}
		if idx := strings.Index(countryName, " ("); idx != -1 {
			countryName = countryName[:idx]
		}
		if strings.Contains(countryName, "Hong Kong") {
			countryName = "Hong Kong"
		}
		if strings.Contains(countryName, "Macao") {
			countryName = "Macao"
		}
		if strings.Contains(countryName, "Taiwan") {
			countryName = "Taiwan"
		}
		const maxLen = 18
		if len(countryName) > maxLen {
			countryName = countryName[:maxLen-3] + "..."
		}

		vh := &VisualHub{
			CC:          cc,
			CountryStr:  countryName,
			DisplayY:    targetY,
			TargetY:     targetY,
			Alpha:       0,
			TargetAlpha: 1.0,
			Rate:        state.hubCurrent[i].rate,
			RateStr:     fmt.Sprintf("%.0f", state.hubCurrent[i].rate),
			Active:      true,
		}
		vh.RateWidth, _ = text.Measure(vh.RateStr, e.subMonoFace, 0)
		activeHubs = append(activeHubs, vh)
	}
	return activeHubs
}

func (e *Engine) gatherActiveImpactsWorker(state *statsWorkerState) []*VisualImpact {
	clear(state.impactMap)

	// Aggregate prefix activity over 60s
	prefixMsgCounts := make(map[string]int)
	prefixClass := make(map[string]bgp.ClassificationType)

	for i := 0; i < 60; i++ {
		b := &state.buckets[i]
		for et, prefixes := range b.anomalyActivity {
			for p, count := range prefixes {
				prefixMsgCounts[p] += count
				// Keep the highest priority classification for the prefix seen in the window
				if e.GetPriority(et.String()) >= e.GetPriority(prefixClass[p].String()) {
					prefixClass[p] = et
				}
			}
		}
	}

	// Also include prefixes that are currently classified even if silent in the window
	// This ensures outages stay in the list until they recover
	now := e.Now()
	for p, st := range state.prefixToClassification {
		if now.Sub(st.ts) > 10*time.Minute {
			delete(state.prefixToClassification, p)
			continue
		}
		if st.ct == bgp.ClassificationNone {
			continue
		}
		// Only override if not already in prefixClass or if higher priority
		if e.GetPriority(st.ct.String()) >= e.GetPriority(prefixClass[p].String()) {
			prefixClass[p] = st.ct
		}
	}

	for p, et := range prefixClass {
		_, name, _ := e.getClassificationVisuals(et)
		if name == "" {
			continue
		}

		visI, ok := state.impactMap[p]
		if !ok {
			visI, ok = state.visualImpact[p]
			if !ok {
				visI = &VisualImpact{Prefix: p, MaskLen: getMaskLen(p)}
				state.visualImpact[p] = visI
			}
			visI.ClassificationName = name
			visI.ClassificationColor, _, _ = e.getClassificationVisuals(et)
			visI.Count = 0
			state.impactMap[p] = visI
		}

		// Rate is messages per second over 60s
		visI.Count = float64(prefixMsgCounts[p]) / 60.0
	}

	allImpact := make([]*VisualImpact, 0, len(state.impactMap))
	for _, visI := range state.impactMap {
		allImpact = append(allImpact, visI)
	}
	return allImpact
}

func (e *Engine) calculatePrefixCounts(state *statsWorkerState, allImpact []*VisualImpact) []PrefixCount {
	clear(state.countMap)
	for _, m := range state.asnsPerClass {
		clear(m)
	}
	clear(state.asnsPerClass)

	allClasses := []bgp.ClassificationType{
		bgp.ClassificationHijack, bgp.ClassificationRouteLeak, bgp.ClassificationOutage,
		bgp.ClassificationFlap, bgp.ClassificationDDoSMitigation, bgp.ClassificationTrafficEngineering,
		bgp.ClassificationPathHunting, bgp.ClassificationDiscovery,
	}
	for _, ct := range allClasses {
		name := ct.String()
		prio := e.GetPriority(name)
		state.countMap[name] = &PrefixCount{
			Name:     name,
			Count:    0,
			Rate:     0,
			Color:    e.getClassificationUIColor(name),
			Priority: prio,
			Type:     ct,
		}
	}

	for _, visI := range allImpact {
		if visI.ClassificationName == "" {
			continue
		}
		asn := state.prefixToASN[visI.Prefix]
		if pc, ok := state.countMap[visI.ClassificationName]; ok {
			pc.Count++
			pc.Rate += visI.Count
			pc.IPCount += utils.GetPrefixSize(visI.Prefix)
		}
		m, ok := state.asnsPerClass[visI.ClassificationName]
		if !ok {
			m = make(map[uint32]struct{})
			state.asnsPerClass[visI.ClassificationName] = m
		}
		m[asn] = struct{}{}
	}

	prefixCounts := make([]PrefixCount, 0, len(state.countMap))
	for name, pc := range state.countMap {
		pc.ASNCount = len(state.asnsPerClass[name])
		pc.ASNStr = strconv.Itoa(pc.ASNCount)
		pc.CountStr = strconv.Itoa(pc.Count)
		pc.IPStr = utils.FormatShortNumber(pc.IPCount)
		pc.RateStr = fmt.Sprintf("%.0f", pc.Rate)

		pc.RateWidth, _ = text.Measure(pc.RateStr, e.subMonoFace, 0)
		pc.ASNWidth, _ = text.Measure(pc.ASNStr, e.subMonoFace, 0)
		pc.CountWidth, _ = text.Measure(pc.CountStr, e.subMonoFace, 0)
		pc.IPWidth, _ = text.Measure(pc.IPStr, e.subMonoFace, 0)
		prefixCounts = append(prefixCounts, *pc)
	}

	sort.Slice(prefixCounts, func(i, j int) bool {
		if prefixCounts[i].Priority != prefixCounts[j].Priority {
			return prefixCounts[i].Priority > prefixCounts[j].Priority
		}
		if prefixCounts[i].Count != prefixCounts[j].Count {
			return prefixCounts[i].Count > prefixCounts[j].Count
		}
		return prefixCounts[i].Name < prefixCounts[j].Name
	})
	return prefixCounts
}

func (e *Engine) calculateASNImpacts(state *statsWorkerState, allImpact []*VisualImpact) []*ASNImpact {
	e.groupASNImpactsWorker(state, allImpact)
	e.sortASNGroupsWorker(state)
	return e.buildActiveASNImpactsWorker(state)
}

func (e *Engine) groupASNImpactsWorker(state *statsWorkerState, allImpact []*VisualImpact) {

	clear(state.asnGroups)
	for _, visI := range allImpact {
		prio := e.GetPriority(visI.ClassificationName)
		if prio < 1 {
			continue
		}
		asn := state.prefixToASN[visI.Prefix]
		if asn == 0 && visI.LeakerASN != 0 {
			asn = visI.LeakerASN
		}
		if asn == 0 {
			continue
		}
		key := asnGroupKey{ASN: asn, Anom: visI.ClassificationName}
		g, ok := state.asnGroups[key]
		if !ok {
			networkName := ""
			if e.asnMapping != nil {
				networkName = e.asnMapping.GetName(asn)
			}
			asnStr := fmt.Sprintf("AS%d", asn)
			if networkName != "" {
				asnStr = fmt.Sprintf("AS%d - %s", asn, networkName)
			}
			g = &asnGroup{
				asnStr:    asnStr,
				anom:      visI.ClassificationName,
				color:     e.getClassificationUIColor(visI.ClassificationName),
				priority:  prio,
				maxCount:  visI.Count,
				prefixes:  make([]string, 0, 4),
				locations: make(map[string]struct{}),
			}
			state.asnGroups[key] = g
		}

		if visI.Count > g.maxCount {
			g.maxCount = visI.Count
		}
		g.totalCount += visI.Count

		if visI.LeakType != bgp.LeakUnknown {
			g.leakType = visI.LeakType
			g.leakerASN = visI.LeakerASN
			g.victimASN = visI.VictimASN
		}

		for cc := range visI.CCs {
			g.locations[cc] = struct{}{}
		}
		g.prefixes = append(g.prefixes, visI.Prefix)
	}

}

func (e *Engine) sortASNGroupsWorker(state *statsWorkerState) {
	state.asnSortedGroups = state.asnSortedGroups[:0]
	for _, g := range state.asnGroups {
		state.asnSortedGroups = append(state.asnSortedGroups, g)
	}
	sort.Slice(state.asnSortedGroups, func(i, j int) bool {
		if state.asnSortedGroups[i].priority != state.asnSortedGroups[j].priority {
			return state.asnSortedGroups[i].priority > state.asnSortedGroups[j].priority
		}
		return state.asnSortedGroups[i].totalCount > state.asnSortedGroups[j].totalCount
	})

}

func (e *Engine) buildActiveASNImpactsWorker(state *statsWorkerState) []*ASNImpact {
	activeASNImpacts := make([]*ASNImpact, 0, 5)
	for i := 0; i < len(state.asnSortedGroups) && i < 5; i++ {
		g := state.asnSortedGroups[i]
		displayPrefixes := g.prefixes
		moreCount := 0
		if len(displayPrefixes) > 1 {
			moreCount = len(displayPrefixes) - 1
			displayPrefixes = displayPrefixes[:1]
		}
		moreStr := ""
		if moreCount > 0 {
			moreStr = fmt.Sprintf("(%d more)", moreCount)
		}
		anomWidth, _ := text.Measure(g.anom, e.subMonoFace, 0)

		locs := make([]string, 0, len(g.locations))
		for cc := range g.locations {
			locs = append(locs, cc)
		}
		sort.Strings(locs)
		locStr := strings.Join(locs, ", ")

		activeASNImpacts = append(activeASNImpacts, &ASNImpact{
			ASNStr:    g.asnStr,
			Prefixes:  displayPrefixes,
			MoreStr:   moreStr,
			Anom:      g.anom,
			AnomWidth: anomWidth,
			Color:     g.color,
			Count:     len(g.prefixes),
			Rate:      g.totalCount,
			LeakType:  g.leakType,
			LeakerASN: g.leakerASN,
			VictimASN: g.victimASN,
			Locations: locStr,
		})
	}
	return activeASNImpacts
}

func (e *Engine) processStatsEvent(msg *statsEvent, state *statsWorkerState) {
	ev := msg.ev
	if ev.prefix != "" {
		if ev.asn != 0 {
			state.prefixToASN[ev.prefix] = ev.asn
		}
		state.prefixToClassification[ev.prefix] = prefixClassState{
			ct: ev.classificationType,
			ts: e.Now(),
		}

		// Track activity for this classification in the current rolling window bucket
		if ev.classificationType != bgp.ClassificationNone {
			bucket := &state.buckets[state.currentIdx]
			if bucket.anomalyActivity[ev.classificationType] == nil {
				bucket.anomalyActivity[ev.classificationType] = make(map[string]int)
			}
			bucket.anomalyActivity[ev.classificationType][ev.prefix]++
		}

		// Update geographic metadata for the prefix
		visI, ok := state.visualImpact[ev.prefix]
		if !ok {
			visI = &VisualImpact{Prefix: ev.prefix, CCs: make(map[string]struct{})}
			state.visualImpact[ev.prefix] = visI
		}
		if visI.CCs == nil {
			visI.CCs = make(map[string]struct{})
		}
		if ev.cc != "" {
			visI.CCs[ev.cc] = struct{}{}
		}
	}
	if ev.cc != "" {
		state.buckets[state.currentIdx].countryActivity[ev.cc]++
	}
}

func (e *Engine) generateInsights(state *statsWorkerState, prefixCounts []PrefixCount) {
	now := e.Now()

	e.streamMu.Lock()
	defer e.streamMu.Unlock()

	var newInsights []*InsightEvent

	canEmit := func(category string, cooldown time.Duration) bool {
		if last, ok := e.lastInsights[category]; ok {
			if now.Sub(last) < cooldown {
				return false
			}
		}
		return true
	}

	recordInsight := func(ie *InsightEvent) {
		e.lastInsights[ie.Category] = now
		newInsights = append(newInsights, ie)
	}

	// 1. Check for Active Outages
	for _, pc := range prefixCounts {
		if pc.Type == bgp.ClassificationOutage && pc.Count > 0 {
			if pc.IPCount >= 1000 && canEmit("Outage", 60*time.Second) {
				// Find worst ASN for outage
				var worstASN *asnGroup
				for _, g := range state.asnSortedGroups {
					if g.anom == bgp.NameHardOutage {
						worstASN = g
						break
					}
				}

				lines := []InsightLine{
					{Label: "  Impacted:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%d ASNs, %s IPs", pc.ASNCount, utils.FormatShortNumber(pc.IPCount)), ValueColor: color.RGBA{255, 255, 0, 255}},
					{Label: "  Networks:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%d prefixes offline", pc.Count), ValueColor: color.RGBA{255, 255, 0, 255}},
				}

				if worstASN != nil {
					lines = append(lines, InsightLine{
						Label: " Worst ASN:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: worstASN.asnStr, ValueColor: color.RGBA{255, 255, 0, 255},
					})
				}

				recordInsight(&InsightEvent{
					Timestamp:  now,
					Category:   "Outage",
					Title:      "MAJOR OUTAGE DETECTED",
					TitleColor: pc.Color,
					Lines:      lines,
				})
			}
		}
	}

	// 2. Check for DDoS Mitigation
	for _, pc := range prefixCounts {
		if pc.Type == bgp.ClassificationDDoSMitigation && pc.Count > 0 {
			if canEmit("DDoS", 120*time.Second) {
				lines := []InsightLine{
					{Label: "Mitigating:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%d prefixes", pc.Count), ValueColor: color.RGBA{255, 255, 0, 255}},
					{Label: "      ASNs:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%d distinct ASNs", pc.ASNCount), ValueColor: color.RGBA{255, 255, 0, 255}},
				}

				recordInsight(&InsightEvent{
					Timestamp:  now,
					Category:   "DDoS",
					Title:      "DDOS MITIGATION ACTIVE",
					TitleColor: pc.Color,
					Lines:      lines,
				})
			}
		}
	}

	// 3. High Churn / Noisiest ASN
	e.metricsMu.Lock()
	churnRate := e.rateUpd + e.rateWith
	e.metricsMu.Unlock()

	if churnRate > 500 && canEmit("Churn", 60*time.Second) {
		lines := []InsightLine{
			{Label: "    Global:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%.0f msgs/sec", churnRate), ValueColor: color.RGBA{255, 255, 0, 255}},
		}

		if len(state.asnSortedGroups) > 0 {
			noisiest := state.asnSortedGroups[0]
			lines = append(lines, InsightLine{
				Label: "  Noisiest:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%s (%.0f events)", noisiest.asnStr, noisiest.totalCount), ValueColor: color.RGBA{255, 255, 0, 255},
			})
		}

		recordInsight(&InsightEvent{
			Timestamp:  now,
			Category:   "Churn",
			Title:      "HIGH BGP CHURN",
			TitleColor: color.RGBA{255, 165, 0, 255}, // Orange
			Lines:      lines,
		})
	}

	// Prepend new insights to the stream
	if len(newInsights) > 0 {
		e.InsightStream = append(newInsights, e.InsightStream...)
		if len(e.InsightStream) > 100 {
			e.InsightStream = e.InsightStream[:100]
		}
		e.streamDirty = true
	}
}
