package bgpengine

import (
	"fmt"
	"image/color"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/biter777/countries"
	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/text/v2"
	"github.com/hajimehoshi/ebiten/v2/vector"
	"github.com/sudorandom/bgp-stream/pkg/utils"
)

func (e *Engine) drawMetrics(screen *ebiten.Image) {
	if e.fontSource == nil {
		return
	}
	margin, fontSize := 40.0, 18.0
	if e.Width > 2000 {
		margin, fontSize = 80.0, 36.0
	}
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()
	face := &text.GoTextFace{Source: e.fontSource, Size: fontSize}

	// 1. West Side: Stable Hub List
	hubYBase := float64(e.Height) / 2.0
	hubX := margin
	boxW, boxH := 280.0, 180.0
	if e.Width > 2000 {
		boxW, boxH = 560.0, 360.0
	}

	if len(e.VisualHubs) > 0 {
		// Draw styled background box
		vector.DrawFilledRect(screen, float32(hubX-10), float32(hubYBase-fontSize-15), float32(boxW), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(screen, float32(hubX-10), float32(hubYBase-fontSize-15), float32(boxW), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

		titleLabel := "TOP ACTIVITY HUBS (ops/s)"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

		// Draw subtle hacker-green accent next to title
		vector.DrawFilledRect(screen, float32(hubX-10), float32(hubYBase-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

		titleOp := &text.DrawOptions{}
		titleOp.GeoM.Translate(hubX+5, hubYBase-fontSize-5)
		titleOp.ColorScale.Scale(1, 1, 1, 0.5)
		text.Draw(screen, titleLabel, titleFace, titleOp)
	}

	for _, vh := range e.VisualHubs {
		countryName := countries.ByName(vh.CC).String()
		if countryName == "Unknown" {
			countryName = vh.CC
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

		// Truncate long names to fit in the box
		const maxLen = 18
		if len(countryName) > maxLen {
			countryName = countryName[:maxLen-3] + "..."
		}

		// Draw Country Name (Left Aligned)
		nameOp := &text.DrawOptions{}
		nameOp.GeoM.Translate(hubX, vh.DisplayY)
		nameOp.ColorScale.Scale(1, 1, 1, float32(vh.Alpha*0.8))
		text.Draw(screen, countryName, face, nameOp)

		// Draw Rate (Right Aligned)
		rateStr := fmt.Sprintf("%.1f", vh.Rate)
		tw, _ := text.Measure(rateStr, face, 0)
		rateOp := &text.DrawOptions{}
		// Position at box right edge minus some padding
		rateOp.GeoM.Translate(hubX+boxW-tw-25, vh.DisplayY)
		rateOp.ColorScale.Scale(1, 1, 1, float32(vh.Alpha*0.6))
		text.Draw(screen, rateStr, face, rateOp)
	}

	// 2. West Side: Most Active Prefixes
	impactYBase := hubYBase + 200.0
	if e.Width > 2000 {
		impactYBase = hubYBase + 400.0
	}

	if len(e.VisualImpact) > 0 {
		// Draw styled background box
		vector.DrawFilledRect(screen, float32(hubX-10), float32(impactYBase-fontSize-15), float32(boxW), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(screen, float32(hubX-10), float32(impactYBase-fontSize-15), float32(boxW), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

		impactTitle := "MOST ACTIVE PREFIXES (ops/s)"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

		// Draw subtle hacker-green accent
		vector.DrawFilledRect(screen, float32(hubX-10), float32(impactYBase-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

		impactOp := &text.DrawOptions{}
		impactOp.GeoM.Translate(hubX+5, impactYBase-fontSize-5)
		impactOp.ColorScale.Scale(1, 1, 1, 0.5)
		text.Draw(screen, impactTitle, titleFace, impactOp)
	}

	for _, vi := range e.VisualImpact {
		// Draw Prefix (Left Aligned)
		prefixOp := &text.DrawOptions{}
		prefixOp.GeoM.Translate(hubX, vi.DisplayY)
		prefixOp.ColorScale.Scale(1, 1, 1, float32(vi.Alpha*0.8))
		text.Draw(screen, vi.Prefix, face, prefixOp)

		// Draw Count (Right Aligned)
		countStr := fmt.Sprintf("%.1f", vi.Count)
		tw, _ := text.Measure(countStr, face, 0)
		countOp := &text.DrawOptions{}
		countOp.GeoM.Translate(hubX+boxW-tw-25, vi.DisplayY)
		countOp.ColorScale.Scale(1, 1, 1, float32(vi.Alpha*0.6))
		text.Draw(screen, countStr, face, countOp)
	}

	// 3. Bottom Right (to the left of graphs): Global Event Rate
	graphW, graphH := 300.0, 100.0
	if e.Width > 2000 {
		graphW, graphH = 600.0, 200.0
	}
	firehoseX := float64(e.Width) - margin - graphW - 320.0
	if e.Width > 2000 {
		firehoseX = float64(e.Width) - margin - graphW - 640.0
	}
	firehoseY := float64(e.Height) - margin - graphH

	imgW, _ := e.pulseImage.Bounds().Dx(), e.pulseImage.Bounds().Dy()
	halfW := float64(imgW) / 2
	swatchSize := fontSize

	drawRow := func(label string, val float64, col color.RGBA, uiCol color.RGBA, y float64) {
		// Draw the pulse circle (swatch) - using the map color (col)
		r, g, b := float32(col.R)/255.0, float32(col.G)/255.0, float32(col.B)/255.0
		baseAlpha := float32(0.6)
		if col == ColorGossip {
			baseAlpha = 0.2
		}

		op := &ebiten.DrawImageOptions{}
		op.Blend = ebiten.BlendLighter
		scale := swatchSize / float64(imgW) * 1.6
		op.GeoM.Translate(-halfW, -halfW)
		op.GeoM.Scale(scale, scale)
		op.GeoM.Translate(firehoseX+(swatchSize/2), y+(fontSize/2))
		op.ColorScale.Scale(r*baseAlpha, g*baseAlpha, b*baseAlpha, baseAlpha)
		screen.DrawImage(e.pulseImage, op)

		// Draw the text - using the UI color (uiCol)
		tr, tg, tb := float32(uiCol.R)/255.0, float32(uiCol.G)/255.0, float32(uiCol.B)/255.0
		top := &text.DrawOptions{}
		top.GeoM.Translate(firehoseX+swatchSize+15, y)
		top.ColorScale.Scale(tr, tg, tb, 0.9)
		text.Draw(screen, fmt.Sprintf("%s: %.1f ops/s", label, val), face, top)
	}

	drawRow("PROPAGATION", e.rateGossip, ColorGossip, ColorGossipUI, firehoseY)
	drawRow("DISCOVERY", e.rateNew, ColorNew, ColorNewUI, firehoseY+fontSize+10)
	drawRow("PATH CHANGE", e.rateUpd, ColorUpd, ColorUpdUI, firehoseY+(fontSize+10)*2)
	drawRow("WITHDRAWAL", e.rateWith, ColorWith, ColorWithUI, firehoseY+(fontSize+10)*3)

	e.drawTrendlines(screen, margin)
}

func (e *Engine) drawTrendlines(screen *ebiten.Image, margin float64) {
	graphW, graphH := 300.0, 100.0
	if e.Width > 2000 {
		graphW, graphH = 600.0, 200.0
	}
	gx, gy := float64(e.Width)-margin-graphW, float64(e.Height)-margin-graphH
	vector.DrawFilledRect(screen, float32(gx), float32(gy), float32(graphW), float32(graphH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(screen, float32(gx), float32(gy), float32(graphW), float32(graphH), 1, color.RGBA{36, 42, 53, 255}, false)
	if len(e.history) < 2 {
		return
	}

	// Helper for log scaling
	logVal := func(v int) float64 {
		if v <= 0 {
			return 0
		}
		return math.Log10(float64(v) + 1.0)
	}

	// Calculate the global maximum log across all time series to use as a shared scale
	globalMaxLog := 1.0 // Minimum ceiling of 10 events (log10(10)=1)
	for _, s := range e.history {
		if l := logVal(s.New); l > globalMaxLog {
			globalMaxLog = l
		}
		if l := logVal(s.Upd); l > globalMaxLog {
			globalMaxLog = l
		}
		if l := logVal(s.With); l > globalMaxLog {
			globalMaxLog = l
		}
		if l := logVal(s.Gossip); l > globalMaxLog {
			globalMaxLog = l
		}
	}

	drawLayer := func(getValue func(s MetricSnapshot) int, col color.RGBA) {
		step := graphW / 60.0
		for i := 0; i < len(e.history)-1; i++ {
			x1, x2 := gx+float64(i)*step, gx+float64(i+1)*step
			// Shared logarithmic scale
			y1 := gy + graphH - (logVal(getValue(e.history[i]))/globalMaxLog)*graphH
			y2 := gy + graphH - (logVal(getValue(e.history[i+1]))/globalMaxLog)*graphH
			vector.StrokeLine(screen, float32(x1), float32(y1), float32(x2), float32(y2), 2, col, false)
		}
	}
	drawLayer(func(s MetricSnapshot) int { return s.Gossip }, ColorGossipUI)
	drawLayer(func(s MetricSnapshot) int { return s.New }, ColorNewUI)
	drawLayer(func(s MetricSnapshot) int { return s.Upd }, ColorUpdUI)
	drawLayer(func(s MetricSnapshot) int { return s.With }, ColorWithUI)
}

func (e *Engine) StartMetricsLoop() {
	firstRun := true
	ticker := time.NewTicker(5 * time.Second)
	run := func() {
		e.metricsMu.Lock()
		defer e.metricsMu.Unlock()

		now := time.Now()
		interval := now.Sub(e.lastMetricsUpdate).Seconds()
		if interval <= 0 {
			interval = 5.0
		}
		e.lastMetricsUpdate = now

		snap := MetricSnapshot{
			New:    int(e.windowNew),
			Upd:    int(e.windowUpd),
			With:   int(e.windowWith),
			Gossip: int(e.windowGossip),
			Note:   int(e.windowNote),
			Peer:   int(e.windowPeer),
			Open:   int(e.windowOpen),
		}
		e.rateNew, e.rateUpd, e.rateWith, e.rateGossip = float64(snap.New)/interval, float64(snap.Upd)/interval, float64(snap.With)/interval, float64(snap.Gossip)/interval
		e.rateNote, e.ratePeer, e.rateOpen = float64(snap.Note)/interval, float64(snap.Peer)/interval, float64(snap.Open)/interval
		e.history = append(e.history, snap)
		if len(e.history) > 60 {
			e.history = e.history[1:]
		}
		for len(e.history) < 60 {
			e.history = append([]MetricSnapshot{{}}, e.history...)
		}
		e.windowNew, e.windowUpd, e.windowWith, e.windowGossip = 0, 0, 0, 0
		e.windowNote, e.windowPeer, e.windowOpen = 0, 0, 0
		type hub struct {
			cc   string
			rate float64
		}
		var current []hub
		for cc, val := range e.countryActivity {
			current = append(current, hub{cc, float64(val) / interval})
		}
		sort.Slice(current, func(i, j int) bool { return current[i].rate > current[j].rate })
		maxItems := 5
		if len(current) < maxItems {
			maxItems = len(current)
		}

		fontSize := 18.0
		if e.Width > 2000 {
			fontSize = 36.0
		}
		spacing := fontSize * 1.2
		hubYBase := float64(e.Height)/2.0 + 10.0

		// Mark all current hubs as inactive so they fade out if not refreshed
		for _, vh := range e.VisualHubs {
			vh.Active = false
			vh.TargetAlpha = 0.0
		}

		for i := 0; i < maxItems; i++ {
			if current[i].rate < 0.1 && !firstRun {
				continue
			}

			targetY := hubYBase + float64(i)*spacing
			if vh, ok := e.VisualHubs[current[i].cc]; ok {
				// Hub already exists, update its target position and rate
				vh.Active = true
				vh.TargetY = targetY
				vh.TargetAlpha = 1.0
				vh.Rate = current[i].rate
			} else {
				// New hub, fade in from the bottom
				e.VisualHubs[current[i].cc] = &VisualHub{
					CC:          current[i].cc,
					Rate:        current[i].rate,
					DisplayY:    hubYBase + float64(maxItems+1)*spacing,
					TargetY:     targetY,
					Alpha:       0,
					TargetAlpha: 1.0,
					Active:      true,
				}
			}
		}

		// Remove any hubs that were not refreshed in this cycle instantly
		for cc, vh := range e.VisualHubs {
			if !vh.Active {
				delete(e.VisualHubs, cc)
			}
		}

		// 2. Process Most Active Prefixes (Current refresh interval rate)
		type impact struct {
			prefix string
			rate   float64
		}
		
		// Use only the latest bucket for the current 5-second interval
		latestBucket := e.prefixImpactHistory[len(e.prefixImpactHistory)-1]
		var allImpact []impact
		for p, count := range latestBucket {
			if utils.IsBeaconPrefix(p) {
				continue
			}
			allImpact = append(allImpact, impact{p, float64(count) / interval})
		}
		sort.Slice(allImpact, func(i, j int) bool { return allImpact[i].rate > allImpact[j].rate })

		maxImpact := 5
		if len(allImpact) < maxImpact {
			maxImpact = len(allImpact)
		}

		impactYBase := hubYBase + 200.0 // Positioned under the hubs box
		if e.Width > 2000 {
			impactYBase = hubYBase + 400.0
		}

		// Mark all current impact items as inactive
		for _, vi := range e.VisualImpact {
			vi.Active = false
			vi.TargetAlpha = 0.0
		}

		for i := 0; i < maxImpact; i++ {
			if allImpact[i].rate < 0.1 && !firstRun {
				continue
			}

			targetY := impactYBase + float64(i)*spacing
			if vi, ok := e.VisualImpact[allImpact[i].prefix]; ok {
				vi.Active = true
				vi.TargetY = targetY
				vi.TargetAlpha = 1.0
				vi.Count = allImpact[i].rate
			} else {
				e.VisualImpact[allImpact[i].prefix] = &VisualImpact{
					Prefix:      allImpact[i].prefix,
					Count:       allImpact[i].rate,
					DisplayY:    impactYBase + float64(maxImpact+1)*spacing,
					TargetY:     targetY,
					Alpha:       0,
					TargetAlpha: 1.0,
					Active:      true,
				}
			}
		}

		// Remove any impact items that were not refreshed in this cycle instantly
		for p, vi := range e.VisualImpact {
			if !vi.Active {
				delete(e.VisualImpact, p)
			}
		}

		// Rotate buckets: discard oldest, add fresh one for next window
		e.prefixImpactHistory = append(e.prefixImpactHistory[1:], make(map[string]int))

		e.countryActivity = make(map[string]int)
		firstRun = false
	}

	go func() {
		time.Sleep(2 * time.Second)
		run()
	}()

	for range ticker.C {
		run()
	}
}
