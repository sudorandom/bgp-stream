package main

import (
	"io/ioutil"
	"strings"
)

func main() {
	content, _ := ioutil.ReadFile("pkg/bgpengine/stats_worker.go")
	lines := strings.Split(string(content), "\n")

	newLines := []string{}
	skip := false
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "func (e *Engine) generateInsights") {
			skip = true
			newLines = append(newLines, `func (e *Engine) canEmitInsight(category string, cooldown time.Duration, now time.Time) bool {
	if last, ok := e.lastInsights[category]; ok {
		if now.Sub(last) < cooldown {
			return false
		}
	}
	return true
}

func (e *Engine) checkOutageInsights(state *statsWorkerState, prefixCounts []PrefixCount, now time.Time) *InsightEvent {
	for i := range prefixCounts {
		pc := &prefixCounts[i]
		if pc.Type == bgp.ClassificationOutage && pc.Count > 0 {
			if pc.IPCount >= 1000 && e.canEmitInsight("Outage", 60*time.Second, now) {
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
				return &InsightEvent{
					Timestamp:  now,
					Category:   "Outage",
					Title:      "MAJOR OUTAGE DETECTED",
					TitleColor: pc.Color,
					Lines:      lines,
				}
			}
		}
	}
	return nil
}

func (e *Engine) checkDDoSInsights(prefixCounts []PrefixCount, now time.Time) *InsightEvent {
	for i := range prefixCounts {
		pc := &prefixCounts[i]
		if pc.Type == bgp.ClassificationDDoSMitigation && pc.Count > 0 {
			if e.canEmitInsight("DDoS", 120*time.Second, now) {
				lines := []InsightLine{
					{Label: "Mitigating:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%d prefixes", pc.Count), ValueColor: color.RGBA{255, 255, 0, 255}},
					{Label: "      ASNs:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%d distinct ASNs", pc.ASNCount), ValueColor: color.RGBA{255, 255, 0, 255}},
				}
				return &InsightEvent{
					Timestamp:  now,
					Category:   "DDoS",
					Title:      "DDOS MITIGATION ACTIVE",
					TitleColor: pc.Color,
					Lines:      lines,
				}
			}
		}
	}
	return nil
}

func (e *Engine) checkChurnInsights(state *statsWorkerState, now time.Time) *InsightEvent {
	e.metricsMu.Lock()
	churnRate := e.rateUpd + e.rateWith
	e.metricsMu.Unlock()

	if churnRate > 500 && e.canEmitInsight("Churn", 60*time.Second, now) {
		lines := []InsightLine{
			{Label: "    Global:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%.0f msgs/sec", churnRate), ValueColor: color.RGBA{255, 255, 0, 255}},
		}

		if len(state.asnSortedGroups) > 0 {
			noisiest := state.asnSortedGroups[0]
			lines = append(lines, InsightLine{
				Label: "  Noisiest:", LabelColor: color.RGBA{180, 180, 180, 255}, Value: fmt.Sprintf("%s (%.0f events)", noisiest.asnStr, noisiest.totalCount), ValueColor: color.RGBA{255, 255, 0, 255},
			})
		}

		return &InsightEvent{
			Timestamp:  now,
			Category:   "Churn",
			Title:      "HIGH BGP CHURN",
			TitleColor: color.RGBA{255, 165, 0, 255}, // Orange
			Lines:      lines,
		}
	}
	return nil
}

func (e *Engine) generateInsights(state *statsWorkerState, prefixCounts []PrefixCount) {
	now := e.Now()

	e.streamMu.Lock()
	defer e.streamMu.Unlock()

	var newInsights []*InsightEvent

	if ie := e.checkOutageInsights(state, prefixCounts, now); ie != nil {
		e.lastInsights[ie.Category] = now
		newInsights = append(newInsights, ie)
	}

	if ie := e.checkDDoSInsights(prefixCounts, now); ie != nil {
		e.lastInsights[ie.Category] = now
		newInsights = append(newInsights, ie)
	}

	if ie := e.checkChurnInsights(state, now); ie != nil {
		e.lastInsights[ie.Category] = now
		newInsights = append(newInsights, ie)
	}

	if len(newInsights) > 0 {
		e.InsightStream = append(newInsights, e.InsightStream...)
		if len(e.InsightStream) > 100 {
			e.InsightStream = e.InsightStream[:100]
		}
		// Reset animation offset based on number of new events (approx 100px per event)
		e.streamOffset -= float64(len(newInsights)) * 100.0
		e.streamDirty = true
	}
}`)
		} else if skip && strings.HasPrefix(line, "}") && i == len(lines)-1 {
			// Done
		} else if !skip {
			newLines = append(newLines, line)
		}
	}
	ioutil.WriteFile("pkg/bgpengine/stats_worker.go", []byte(strings.Join(newLines, "\n")), 0644)
}
