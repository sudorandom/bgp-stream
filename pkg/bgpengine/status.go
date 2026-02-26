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
		vector.FillRect(screen, float32(hubX-10), float32(hubYBase-fontSize-15), float32(boxW), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(screen, float32(hubX-10), float32(hubYBase-fontSize-15), float32(boxW), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

		titleLabel := "TOP ACTIVITY HUBS (ops/s)"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

		// Draw subtle hacker-green accent next to title
		vector.FillRect(screen, float32(hubX-10), float32(hubYBase-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

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
		vector.FillRect(screen, float32(hubX-10), float32(impactYBase-fontSize-15), float32(boxW), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(screen, float32(hubX-10), float32(impactYBase-fontSize-15), float32(boxW), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

		impactTitle := "MOST ACTIVE PREFIXES (ops/s)"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

		// Draw subtle hacker-green accent
		vector.FillRect(screen, float32(hubX-10), float32(impactYBase-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

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
		vector.FillRect(screen, float32(hubX-10), float32(songYBase-fontSize-15), float32(songBoxW), float32(boxH_song), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(screen, float32(hubX-10), float32(songYBase-fontSize-15), float32(songBoxW), float32(boxH_song), 1, color.RGBA{36, 42, 53, 255}, false)

		songTitle := "NOW PLAYING"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

		// Draw subtle hacker-green accent
		vector.FillRect(screen, float32(hubX-10), float32(songYBase-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

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
	beaconW := 220.0
	if e.Width > 2000 {
		beaconW = 440.0
	}
	trendBoxW := graphW + ratesW
	// Each box has a width: legendW, trendBoxW+20, beaconW
	totalW := legendW + spacing + (trendBoxW + 20) + spacing + beaconW
	baseX := float64(e.Width) - margin - totalW
	baseY := float64(e.Height) - margin - graphH - 20

	firehoseX := baseX
	firehoseY := baseY
	gx := baseX + legendW + spacing
	gy := baseY

	// Draw Trendlines Box
	e.drawTrendlines(screen, gx, gy, graphW, trendBoxW, graphH, fontSize, legendH)

	// Draw Beacon Analysis Box
	beaconX := gx + trendBoxW + 20 + spacing // Gap between (gx-10+trendBoxW+20) and (beaconX-10) should be exactly spacing
	e.drawBeaconMetrics(screen, beaconX, gy, beaconW, graphH, fontSize, legendH)

	// Draw Legend Box
	vector.FillRect(screen, float32(firehoseX-10), float32(firehoseY-fontSize-15), float32(legendW), float32(legendH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(screen, float32(firehoseX-10), float32(firehoseY-fontSize-15), float32(legendW), float32(legendH), 1, color.RGBA{36, 42, 53, 255}, false)

	legendTitle := "LEGEND"
	titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

	// Draw subtle hacker-green accent
	vector.FillRect(screen, float32(firehoseX-10), float32(firehoseY-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

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

	// Calculate smooth offset for rate interpolation to match the graph
	timeSinceLastUpdate := time.Since(e.lastMetricsUpdate)
	smoothOffset := timeSinceLastUpdate.Seconds()
	if smoothOffset > 1.0 {
		smoothOffset = 1.0
	}

	hLen := len(e.history)
	getRate := func(idx int, getValue func(s MetricSnapshot) int) float64 {
		if hLen < 2 || idx < 0 || idx >= hLen {
			return 0
		}
		// Snaps were 1s apart, so interval is 1.0
		return float64(getValue(e.history[idx])) / 1.0
	}

	type row struct {
		label string
		val   float64
		col   color.RGBA
		uiCol color.RGBA
	}

	// Calculate interpolated rates based on what's currently at the right edge of the graph
	rows := []row{
		{"PROPAGATION", 0, ColorGossip, ColorGossipUI},
		{"PATH CHANGE", 0, ColorUpd, ColorUpdUI},
		{"WITHDRAWAL", 0, ColorWith, ColorWithUI},
		{"NEW PATHS", 0, ColorNew, ColorNewUI},
	}
	accessors := []func(s MetricSnapshot) int{
		func(s MetricSnapshot) int { return s.Gossip },
		func(s MetricSnapshot) int { return s.Upd },
		func(s MetricSnapshot) int { return s.With },
		func(s MetricSnapshot) int { return s.New },
	}
	for i := range rows {
		// Use the stable rate from the previous snapshot (1s ago)
		// to prevent the numbers from spinning/flickering.
		rows[i].val = getRate(hLen-2, accessors[i])
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].val > rows[j].val })

	for i, r := range rows {
		y := firehoseY + float64(i)*(fontSize+10)
		// Draw the pulse circle (swatch) - using the map color (col)
		cr, cg, cb := float32(r.col.R)/255.0, float32(r.col.G)/255.0, float32(r.col.B)/255.0
		baseAlpha := float32(0.6)
		if r.col == ColorGossip {
			baseAlpha = 0.4
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
		rateStr := fmt.Sprintf("%.0f ops/s", r.val)
		if r.val < 10 {
			rateStr = fmt.Sprintf("%.1f ops/s", r.val)
		}
		rateOp := &text.DrawOptions{}
		// Place rate to the right of the graph area
		rateOp.GeoM.Translate(gx+graphW+15, y)
		rateOp.ColorScale.Scale(tr, tg, tb, 0.9)
		text.Draw(screen, rateStr, face, rateOp)
	}
}

func (e *Engine) drawTrendlines(screen *ebiten.Image, gx, gy, graphW, trendBoxW, graphH, fontSize, boxH float64) {
	vector.FillRect(screen, float32(gx-10), float32(gy-fontSize-15), float32(trendBoxW+20), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(screen, float32(gx-10), float32(gy-fontSize-15), float32(trendBoxW+20), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

	trendTitle := "ACTIVITY TREND (1m)"
	titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

	// Draw subtle hacker-green accent
	vector.FillRect(screen, float32(gx-10), float32(gy-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

	trendOp := &text.DrawOptions{}
	trendOp.GeoM.Translate(gx+5, gy-fontSize-5)
	trendOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(screen, trendTitle, titleFace, trendOp)

	if len(e.history) < 2 {
		return
	}

	// Calculate smooth offset based on time since last metrics update (1s interval)
	timeSinceLastUpdate := time.Since(e.lastMetricsUpdate)
	smoothOffset := timeSinceLastUpdate.Seconds()
	if smoothOffset > 1.0 {
		smoothOffset = 1.0
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

	// Create a temporary image for clipping
	trendImg := ebiten.NewImage(int(graphW), int(graphH+10))

	drawLayer := func(getValue func(s MetricSnapshot) int, col color.RGBA) {
		numSteps := float64(len(e.history) - 2)
		step := graphW / numSteps

		for i := 0; i < len(e.history)-1; i++ {
			x1 := (float64(i) - smoothOffset) * step
			x2 := (float64(i+1) - smoothOffset) * step

			// Calculate alpha based on position to create a fade-out on the left
			// Fade starts at 20% of the graph width and reaches 0 at the far left
			alpha := 1.0
			fadeWidth := graphW * 0.2
			if x1 < fadeWidth {
				alpha = x1 / fadeWidth
				if alpha < 0 {
					alpha = 0
				}
			}

			// Shared logarithmic scale
			y1 := graphH - (logVal(getValue(e.history[i]))/globalMaxLog)*graphH
			y2 := graphH - (logVal(getValue(e.history[i+1]))/globalMaxLog)*graphH

			// Manually premultiply the color components to ensure a true fade-out
			// This prevents the lines from appearing "white" as they fade.
			c := color.RGBA{
				R: uint8(float64(col.R) * alpha),
				G: uint8(float64(col.G) * alpha),
				B: uint8(float64(col.B) * alpha),
				A: uint8(float64(col.A) * alpha),
			}
			vector.StrokeLine(trendImg, float32(x1), float32(y1), float32(x2), float32(y2), 2, c, false)
		}
	}
	drawLayer(func(s MetricSnapshot) int { return s.Gossip }, ColorGossipUI)
	drawLayer(func(s MetricSnapshot) int { return s.New }, ColorNewUI)
	drawLayer(func(s MetricSnapshot) int { return s.Upd }, ColorUpdUI)
	drawLayer(func(s MetricSnapshot) int { return s.With }, ColorWithUI)

	// Draw the clipped trend image back to the screen
	op := &ebiten.DrawImageOptions{}
	op.GeoM.Translate(gx, gy)
	screen.DrawImage(trendImg, op)
}

func (e *Engine) StartMetricsLoop() {
	firstRun := true
	ticker := time.NewTicker(1 * time.Second)
	uiTicks := 0
	var lastUIUpdate time.Time

	run := func() {
		e.metricsMu.Lock()
		defer e.metricsMu.Unlock()

		now := time.Now()
		interval := now.Sub(e.lastMetricsUpdate).Seconds()
		if interval <= 0 {
			interval = 1.0
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
			Beacon: int(e.windowBeacon),
		}
		e.rateNew, e.rateUpd, e.rateWith, e.rateGossip = float64(snap.New)/interval, float64(snap.Upd)/interval, float64(snap.With)/interval, float64(snap.Gossip)/interval
		e.rateNote, e.ratePeer, e.rateOpen = float64(snap.Note)/interval, float64(snap.Peer)/interval, float64(snap.Open)/interval
		e.rateBeacon = float64(snap.Beacon) / interval
		e.history = append(e.history, snap)
		if len(e.history) > 60 {
			e.history = e.history[1:]
		}
		for len(e.history) < 60 {
			e.history = append([]MetricSnapshot{{}}, e.history...)
		}
		e.windowNew, e.windowUpd, e.windowWith, e.windowGossip = 0, 0, 0, 0
		e.windowNote, e.windowPeer, e.windowOpen = 0, 0, 0
		e.windowBeacon = 0

		uiTicks++
		if uiTicks >= 5 || firstRun {
			uiInterval := now.Sub(lastUIUpdate).Seconds()
			if firstRun || uiInterval <= 0 {
				uiInterval = 5.0
			}
			lastUIUpdate = now

			type hub struct {
				cc   string
				rate float64
			}
			var current []hub
			for cc, val := range e.countryActivity {
				current = append(current, hub{cc, float64(val) / uiInterval})
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
				allImpact = append(allImpact, impact{p, float64(count) / uiInterval})
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
			uiTicks = 0
		}
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

func (e *Engine) drawBeaconMetrics(screen *ebiten.Image, x, y, w, h, fontSize, boxH float64) {
	vector.FillRect(screen, float32(x-10), float32(y-fontSize-15), float32(w), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(screen, float32(x-10), float32(y-fontSize-15), float32(w), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

	title := "BEACON ANALYSIS"
	titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}

	vector.FillRect(screen, float32(x-10), float32(y-fontSize-15), 4, float32(fontSize+10), color.RGBA{255, 165, 0, 255}, false) // Orange accent

	op := &text.DrawOptions{}
	op.GeoM.Translate(x+5, y-fontSize-5)
	op.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(screen, title, titleFace, op)

	// Donut Pie Chart dimensions
	radius := h * 0.45
	centerX := x + (w / 2) - 10
	centerY := y + (h / 2) - 10

	// Common options
	drawOpts := &vector.DrawPathOptions{AntiAlias: true}
	fillOpts := &vector.FillOptions{}

	// 1. Background circle
	var bgPath vector.Path
	bgPath.Arc(float32(centerX), float32(centerY), float32(radius), 0, 2*math.Pi, vector.Clockwise)
	bgDrawOpts := *drawOpts
	bgDrawOpts.ColorScale.ScaleWithColor(color.RGBA{30, 30, 30, 255})
	vector.FillPath(screen, &bgPath, fillOpts, &bgDrawOpts)

	// 2. Beacon slice
	if e.displayBeaconPercent > 0.01 {
		var beaconPath vector.Path
		startAngle := -math.Pi / 2 // Top
		endAngle := startAngle + (2 * math.Pi * e.displayBeaconPercent / 100.0)

		beaconPath.MoveTo(float32(centerX), float32(centerY))
		beaconPath.Arc(float32(centerX), float32(centerY), float32(radius), float32(startAngle), float32(endAngle), vector.Clockwise)
		beaconPath.Close()

		beaconDrawOpts := *drawOpts
		beaconDrawOpts.ColorScale.ScaleWithColor(color.RGBA{255, 165, 0, 255})
		vector.FillPath(screen, &beaconPath, fillOpts, &beaconDrawOpts)
	}

	// 3. Donut hole (mask)
	var holePath vector.Path
	holeRadius := radius * 0.7
	holePath.Arc(float32(centerX), float32(centerY), float32(holeRadius), 0, 2*math.Pi, vector.Clockwise)
	
	holeDrawOpts := *drawOpts
	holeDrawOpts.ColorScale.ScaleWithColor(color.RGBA{8, 10, 15, 255})
	vector.FillPath(screen, &holePath, fillOpts, &holeDrawOpts)

	// 4. Percentage Text (in middle of donut)
	face := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.9}
	percentStr := fmt.Sprintf("%.1f%%", e.displayBeaconPercent)
	tw, _ := text.Measure(percentStr, face, 0)
	pop := &text.DrawOptions{}
	pop.GeoM.Translate(centerX-(tw/2), centerY-(fontSize/2))
	pop.ColorScale.Scale(1, 1, 1, 0.9)
	text.Draw(screen, percentStr, face, pop)

	// 5. Small Legend Underneath
	subFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.5}
	legY := centerY + radius + fontSize*0.6
	swatchSize := fontSize * 0.4
	
	// Calculate combined width of both legend items
	bStr, oStr := "BEACON", "ORGANIC"
	btw, _ := text.Measure(bStr, subFace, 0)
	otw, _ := text.Measure(oStr, subFace, 0)
	
	// Total width = swatch + gap + text + padding + swatch + gap + text
	itemGap := 20.0
	bItemW := swatchSize + 5 + btw
	oItemW := swatchSize + 5 + otw
	totalLegW := bItemW + itemGap + oItemW
	
	legX := x + (w/2) - (totalLegW/2) - 10 // Center relative to donut center (-10 is from centerX calculation)

	// Beacon Legend Item
	vector.FillRect(screen, float32(legX), float32(legY), float32(swatchSize), float32(swatchSize), color.RGBA{255, 165, 0, 255}, false)
	bop := &text.DrawOptions{}
	bop.GeoM.Translate(legX+swatchSize+5, legY-fontSize*0.1)
	bop.ColorScale.Scale(1, 1, 1, 0.6)
	text.Draw(screen, bStr, subFace, bop)

	// Organic Legend Item
	legX += bItemW + itemGap
	vector.FillRect(screen, float32(legX), float32(legY), float32(swatchSize), float32(swatchSize), color.RGBA{30, 30, 30, 255}, false)
	oop := &text.DrawOptions{}
	oop.GeoM.Translate(legX+swatchSize+5, legY-fontSize*0.1)
	oop.ColorScale.Scale(1, 1, 1, 0.6)
	text.Draw(screen, oStr, subFace, oop)
}
