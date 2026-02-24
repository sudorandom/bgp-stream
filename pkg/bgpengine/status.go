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

	// 4. West Side: Now Playing
	songYBase := impactYBase + 200.0
	if e.Width > 2000 {
		songYBase = impactYBase + 400.0
	}

	if e.CurrentSong != "" {
		songBoxW := boxW * 1.6
		// Calculate height based on font size and whether there is an artist
		boxH_song := fontSize * 3.0
		if e.CurrentArtist != "" {
			boxH_song = fontSize * 4.5
		}

		// Draw styled background box
		vector.DrawFilledRect(screen, float32(hubX-10), float32(songYBase-fontSize-15), float32(songBoxW), float32(boxH_song), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(screen, float32(hubX-10), float32(songYBase-fontSize-15), float32(songBoxW), float32(boxH_song), 1, color.RGBA{36, 42, 53, 255}, false)

		songTitle := "NOW PLAYING"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

		// Draw subtle hacker-green accent
		vector.DrawFilledRect(screen, float32(hubX-10), float32(songYBase-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

		songTitleOp := &text.DrawOptions{}
		songTitleOp.GeoM.Translate(hubX+5, songYBase-fontSize-5)
		songTitleOp.ColorScale.Scale(1, 1, 1, 0.5)
		text.Draw(screen, songTitle, titleFace, songTitleOp)

		now := time.Now()
		glitchDuration := 2 * time.Second
		isGlitching := now.Sub(e.songChangedAt) < glitchDuration
		intensity := 0.0
		if isGlitching {
			intensity = 1.0 - (now.Sub(e.songChangedAt).Seconds() / glitchDuration.Seconds())
		}

		// Helper for marquee drawing
		drawMarquee := func(label string, f *text.GoTextFace, yOffset float64, alpha float32, sub bool, buffer **ebiten.Image) {
			tw, _ := text.Measure(label, f, 0)
			availW := songBoxW - 20
			
			if tw > availW {
				// Marquee effect
				speed := 30.0
				padding := 60.0
				totalW := tw + padding
				offset := math.Mod(time.Since(e.songChangedAt).Seconds()*speed, totalW)
				
				bh := int(f.Size * 1.5)
				bw := int(availW)
				if *buffer == nil || (*buffer).Bounds().Dx() != bw || (*buffer).Bounds().Dy() != bh {
					*buffer = ebiten.NewImage(bw, bh)
				} else {
					(*buffer).Clear()
				}
				
				// Draw text with offset
				op := &text.DrawOptions{}
				op.GeoM.Translate(-offset, 0)
				op.ColorScale.Scale(1, 1, 1, alpha)
				text.Draw(*buffer, label, f, op)
				
				// Draw second copy for seamless loop
				op.GeoM.Reset()
				op.GeoM.Translate(totalW-offset, 0)
				op.ColorScale.Scale(1, 1, 1, alpha)
				text.Draw(*buffer, label, f, op)

				// Draw clipped result to screen
				drawOp := &ebiten.DrawImageOptions{}
				drawOp.GeoM.Translate(hubX, songYBase+yOffset)
				screen.DrawImage(*buffer, drawOp)
			} else {
				if sub {
					e.drawGlitchTextSubtle(screen, label, f, hubX, songYBase+yOffset, alpha, intensity, isGlitching)
				} else {
					e.drawGlitchTextAggressive(screen, label, f, hubX, songYBase+yOffset, alpha, intensity, isGlitching)
				}
			}
		}

		// Draw Song Name
		drawMarquee(e.CurrentSong, face, fontSize*0.2, 0.8, false, &e.songBuffer)

		// Draw Artist Name
		if e.CurrentArtist != "" {
			artistFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.7}
			drawMarquee(e.CurrentArtist, artistFace, fontSize*1.3, 0.5, true, &e.artistBuffer)
		}
	}

	// 3. Bottom Right: Legend & Trendlines
	graphW, graphH := 300.0, 100.0
	ratesW := 120.0
	legendW, legendH := 260.0, 160.0
	if e.Width > 2000 {
		graphW, graphH = 600.0, 200.0
		ratesW = 240.0
		legendW, legendH = 520.0, 320.0
	}

	// Match heights
	trendBoxH := legendH - fontSize - 25 // Calculate inner graph height area to match legend box height
	graphH = trendBoxH - 10              // Leave some room inside

	// Calculate positions: Legend on far left, Trendlines box (containing graph + rates) on right
	spacing := 40.0
	trendBoxW := graphW + ratesW
	totalW := trendBoxW + spacing + legendW
	baseX := float64(e.Width) - margin - totalW
	baseY := float64(e.Height) - margin - graphH - 20

	firehoseX := baseX
	firehoseY := baseY
	gx := baseX + legendW + spacing
	gy := baseY

	// Draw Trendlines Box (passing trendBoxW for the background box)
	e.drawTrendlines(screen, gx, gy, graphW, trendBoxW, graphH, fontSize, legendH)

	// Draw Legend Box
	vector.DrawFilledRect(screen, float32(firehoseX-10), float32(firehoseY-fontSize-15), float32(legendW), float32(legendH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(screen, float32(firehoseX-10), float32(firehoseY-fontSize-15), float32(legendW), float32(legendH), 1, color.RGBA{36, 42, 53, 255}, false)

	legendTitle := "LEGEND"
	titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

	// Draw subtle hacker-green accent
	vector.DrawFilledRect(screen, float32(firehoseX-10), float32(firehoseY-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

	legendOp := &text.DrawOptions{}
	legendOp.GeoM.Translate(firehoseX+5, firehoseY-fontSize-5)
	legendOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(screen, legendTitle, titleFace, legendOp)

	imgW, _ := e.pulseImage.Bounds().Dx(), e.pulseImage.Bounds().Dy()
	halfW := float64(imgW) / 2
	swatchSize := fontSize

	// Shift legend content slightly for padding
	firehoseX += 10
	firehoseY += 10

	type row struct {
		label string
		val   float64
		col   color.RGBA
		uiCol color.RGBA
	}
	rows := []row{
		{"PROPAGATION", e.rateGossip, ColorGossip, ColorGossipUI},
		{"PATH CHANGE", e.rateUpd, ColorUpd, ColorUpdUI},
		{"WITHDRAWAL", e.rateWith, ColorWith, ColorWithUI},
		{"NEW PATHS", e.rateNew, ColorNew, ColorNewUI},
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].val > rows[j].val })

	for i, r := range rows {
		y := firehoseY + float64(i)*(fontSize+10)
		// Draw the pulse circle (swatch) - using the map color (col)
		cr, cg, cb := float32(r.col.R)/255.0, float32(r.col.G)/255.0, float32(r.col.B)/255.0
		baseAlpha := float32(0.6)
		if r.col == ColorGossip {
			baseAlpha = 0.2
		}

		op := &ebiten.DrawImageOptions{}
		op.Blend = ebiten.BlendLighter
		scale := swatchSize / float64(imgW) * 1
		op.GeoM.Translate(-halfW, -halfW)
		op.GeoM.Scale(scale, scale)
		op.GeoM.Translate(firehoseX+(swatchSize/2), y+(fontSize/2))
		op.ColorScale.Scale(cr*baseAlpha, cg*baseAlpha, cb*baseAlpha, baseAlpha)
		screen.DrawImage(e.pulseImage, op)

		// Draw the text label in the legend box
		tr, tg, tb := float32(r.uiCol.R)/255.0, float32(r.uiCol.G)/255.0, float32(r.uiCol.B)/255.0
		top := &text.DrawOptions{}
		top.GeoM.Translate(firehoseX+swatchSize+15, y)
		top.ColorScale.Scale(tr, tg, tb, 0.9)
		text.Draw(screen, r.label, face, top)

		// Draw the numerical rate on the right side of the trendlines box
		rateStr := fmt.Sprintf("%.1f ops/s", r.val)
		rateOp := &text.DrawOptions{}
		// Place rate to the right of the graph area
		rateOp.GeoM.Translate(gx+graphW+15, y)
		rateOp.ColorScale.Scale(tr, tg, tb, 0.9)
		text.Draw(screen, rateStr, face, rateOp)
	}
}

func (e *Engine) drawTrendlines(screen *ebiten.Image, gx, gy, graphW, trendBoxW, graphH, fontSize, boxH float64) {
	vector.DrawFilledRect(screen, float32(gx-10), float32(gy-fontSize-15), float32(trendBoxW+20), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(screen, float32(gx-10), float32(gy-fontSize-15), float32(trendBoxW+20), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

	trendTitle := "ACTIVITY TREND (2m)"
	titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

	// Draw subtle hacker-green accent
	vector.DrawFilledRect(screen, float32(gx-10), float32(gy-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

	trendOp := &text.DrawOptions{}
	trendOp.GeoM.Translate(gx+5, gy-fontSize-5)
	trendOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(screen, trendTitle, titleFace, trendOp)

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
