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

type legendRow struct {
	label    string
	val      float64
	col      color.RGBA
	uiCol    color.RGBA
	accessor func(s MetricSnapshot) int
}

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
	monoFace := &text.GoTextFace{Source: e.monoSource, Size: fontSize}

	// 1. West Side: Stable Hub List
	hubYBase := float64(e.Height) / 2.0
	hubX := margin
	boxW, boxH := 280.0, 180.0
	impactBoxH := 320.0
	if e.Width > 2000 {
		boxW, boxH = 560.0, 360.0
		impactBoxH = 640.0
	}

	if len(e.VisualHubs) > 0 {
		if e.hubsBuffer == nil || e.hubsBuffer.Bounds().Dx() != int(boxW) || e.hubsBuffer.Bounds().Dy() != int(boxH) {
			e.hubsBuffer = ebiten.NewImage(int(boxW), int(boxH))
		}
		e.hubsBuffer.Clear()

		// Draw into buffer using local coordinates (relative to hubX-10, hubYBase-fontSize-15)
		localX, localY := 10.0, fontSize+15.0
		vector.FillRect(e.hubsBuffer, 0, 0, float32(boxW), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(e.hubsBuffer, 0, 0, float32(boxW), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

		titleLabel := "TOP ACTIVITY HUBS (ops/s)"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}
		vector.FillRect(e.hubsBuffer, 0, 0, 4, float32(fontSize+10), ColorNew, false)

		titleOp := &text.DrawOptions{}
		titleOp.GeoM.Translate(localX+5, localY-fontSize-5)
		titleOp.ColorScale.Scale(1, 1, 1, 0.5)
		text.Draw(e.hubsBuffer, titleLabel, titleFace, titleOp)

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
			nameOp.GeoM.Translate(localX, vh.DisplayY-(hubYBase-localY))
			nameOp.ColorScale.Scale(1, 1, 1, float32(vh.Alpha*0.8))
			text.Draw(e.hubsBuffer, countryName, monoFace, nameOp)

			// Draw Rate (Right Aligned)
			rateStr := fmt.Sprintf("%.0f", vh.Rate)
			tw, _ := text.Measure(rateStr, monoFace, 0)
			rateOp := &text.DrawOptions{}
			rateOp.GeoM.Translate(localX+boxW-tw-25, vh.DisplayY-(hubYBase-localY))
			rateOp.ColorScale.Scale(1, 1, 1, float32(vh.Alpha*0.6))
			text.Draw(e.hubsBuffer, rateStr, monoFace, rateOp)
		}

		now := time.Now()
		isHubUpdating := now.Sub(e.hubUpdatedAt) < 2*time.Second
		hubIntensity := 0.0
		if isHubUpdating {
			hubIntensity = 1.0 - (now.Sub(e.hubUpdatedAt).Seconds() / 2.0)
		}
		e.drawGlitchImage(screen, e.hubsBuffer, hubX-10, hubYBase-fontSize-15, 1.0, hubIntensity, isHubUpdating)
	}

	// 2. West Side: Most Active Prefixes
	impactYBase := hubYBase + boxH + 40.0
	if e.Width > 2000 {
		impactYBase = hubYBase + boxH + 80.0
	}

	if len(e.VisualImpact) > 0 {
		if e.impactBuffer == nil || e.impactBuffer.Bounds().Dx() != int(boxW) || e.impactBuffer.Bounds().Dy() != int(impactBoxH) {
			e.impactBuffer = ebiten.NewImage(int(boxW), int(impactBoxH))
		}
		e.impactBuffer.Clear()

		localX, localY := 10.0, fontSize+15.0
		vector.FillRect(e.impactBuffer, 0, 0, float32(boxW), float32(impactBoxH), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(e.impactBuffer, 0, 0, float32(boxW), float32(impactBoxH), 1, color.RGBA{36, 42, 53, 255}, false)

		impactTitle := "MOST ACTIVE PREFIXES (ops/s)"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}
		vector.FillRect(e.impactBuffer, 0, 0, 4, float32(fontSize+10), ColorNew, false)

		impactOp := &text.DrawOptions{}
		impactOp.GeoM.Translate(localX+5, localY-fontSize-5)
		impactOp.ColorScale.Scale(1, 1, 1, 0.5)
		text.Draw(e.impactBuffer, impactTitle, titleFace, impactOp)

		for _, vi := range e.VisualImpact {
			// Draw Prefix (Left Aligned)
			prefixOp := &text.DrawOptions{}
			prefixOp.GeoM.Translate(localX, vi.DisplayY-(impactYBase-localY))
			prefixOp.ColorScale.Scale(1, 1, 1, float32(vi.Alpha*0.8))
			text.Draw(e.impactBuffer, vi.Prefix, monoFace, prefixOp)

			// Draw Count (Right Aligned)
			countStr := fmt.Sprintf("%.0f", vi.Count)
			tw, _ := text.Measure(countStr, monoFace, 0)
			countOp := &text.DrawOptions{}
			countOp.GeoM.Translate(localX+boxW-tw-25, vi.DisplayY-(impactYBase-localY))
			countOp.ColorScale.Scale(1, 1, 1, float32(vi.Alpha*0.6))
			text.Draw(e.impactBuffer, countStr, monoFace, countOp)

			// Draw ASN and Network Name (Subtle line underneath, wrapped)
			if vi.ASN != 0 || vi.NetworkName != "" {
				subFontSize := fontSize * 0.6
				subMonoFace := &text.GoTextFace{Source: e.monoSource, Size: subFontSize}
				asnStr := fmt.Sprintf("AS%d", vi.ASN)
				if vi.NetworkName != "" {
					asnStr = fmt.Sprintf("AS%d - %s", vi.ASN, vi.NetworkName)
				}

				charW, _ := text.Measure("A", subMonoFace, 0)
				maxChars := int((boxW - 25) / charW)
				if maxChars < 10 {
					maxChars = 10
				}

				lines := wrapString(asnStr, maxChars, 2) // Limit to 2 lines
				for j, line := range lines {
					asnOp := &text.DrawOptions{}
					asnOp.GeoM.Translate(localX, vi.DisplayY-(impactYBase-localY)+fontSize*1.2+float64(j)*subFontSize*1.1)
					asnOp.ColorScale.Scale(1, 1, 1, float32(vi.Alpha*0.4))
					text.Draw(e.impactBuffer, line, subMonoFace, asnOp)
				}
			}
		}

		now := time.Now()
		isImpactUpdating := now.Sub(e.impactUpdatedAt) < 2*time.Second
		impactIntensity := 0.0
		if isImpactUpdating {
			impactIntensity = 1.0 - (now.Sub(e.impactUpdatedAt).Seconds() / 2.0)
		}
		e.drawGlitchImage(screen, e.impactBuffer, hubX-10, impactYBase-fontSize-15, 1.0, impactIntensity, isImpactUpdating)
	}

	// 4. Top Right: Now Playing
	songX := float64(e.Width) - margin - (boxW * 1.0)
	songYBase := margin + fontSize + 15

	if e.CurrentSong != "" {
		songBoxW := boxW * 1.0
		// Calculate height based on font size and whether there is an artist/extra
		boxH_song := fontSize * 2.5
		if e.CurrentArtist != "" {
			boxH_song += fontSize * 1.2
		}
		if e.CurrentExtra != "" {
			boxH_song += fontSize * 1.2
		}

		if e.nowPlayingBuffer == nil || e.nowPlayingBuffer.Bounds().Dx() != int(songBoxW) || e.nowPlayingBuffer.Bounds().Dy() != int(boxH_song) {
			e.nowPlayingBuffer = ebiten.NewImage(int(songBoxW), int(boxH_song))
		}
		e.nowPlayingBuffer.Clear()

		localX, localY := 10.0, fontSize+15.0
		vector.FillRect(e.nowPlayingBuffer, 0, 0, float32(songBoxW), float32(boxH_song), color.RGBA{0, 0, 0, 100}, false)
		vector.StrokeRect(e.nowPlayingBuffer, 0, 0, float32(songBoxW), float32(boxH_song), 1, color.RGBA{36, 42, 53, 255}, false)

		songTitle := "NOW PLAYING"
		titleFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.8}
		vector.FillRect(e.nowPlayingBuffer, 0, 0, 4, float32(fontSize+10), ColorNew, false)

		songTitleOp := &text.DrawOptions{}
		songTitleOp.GeoM.Translate(localX, localY-fontSize-5)
		songTitleOp.ColorScale.Scale(1, 1, 1, 0.5)
		text.Draw(e.nowPlayingBuffer, songTitle, titleFace, songTitleOp)

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
			availW := songBoxW - 40

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

				// Draw clipped result to buffer
				drawOp := &ebiten.DrawImageOptions{}
				drawOp.GeoM.Translate(localX, localY+yOffset)
				e.nowPlayingBuffer.DrawImage(*buffer, drawOp)
			} else {
				if isGlitching {
					// Draw glitched text into buffer
					e.drawGlitchTextSubtle(e.nowPlayingBuffer, label, f, localX, localY+yOffset, alpha, intensity, true)
				} else {
					op := &text.DrawOptions{}
					op.GeoM.Translate(localX, localY+yOffset)
					op.ColorScale.Scale(1, 1, 1, alpha)
					text.Draw(e.nowPlayingBuffer, label, f, op)
				}
			}
		}

		// Draw Song Name
		drawMarquee(e.CurrentSong, face, fontSize*0.2, 0.8, false, &e.songBuffer)

		// Draw Artist Name
		yOffset := fontSize * 1.1
		if e.CurrentArtist != "" {
			artistFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.7}
			drawMarquee(e.CurrentArtist, artistFace, yOffset, 0.5, true, &e.artistBuffer)
			yOffset += fontSize * 1.1
		}

		// Draw Extra (Source/License)
		if e.CurrentExtra != "" {
			extraFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.6}
			drawMarquee(e.CurrentExtra, extraFace, yOffset, 0.4, true, &e.extraBuffer)
		}

		e.drawGlitchImage(screen, e.nowPlayingBuffer, songX-10, songYBase-fontSize-15, 1.0, intensity, isGlitching)
	}

	// 3. Bottom Right: Legend & Trendlines
	graphW, graphH := 300.0, 100.0
	legendW, legendH := 260.0, 150.0
	if e.Width > 2000 {
		graphW, graphH = 600.0, 200.0
		legendW, legendH = 520.0, 300.0
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
	// The trend box width includes the graph and the right margin for labels
	trendBoxW := graphW + 60.0
	if e.Width > 2000 {
		trendBoxW = graphW + 120.0
	}
	// Each box has a width: legendW, trendBoxW+20, beaconW
	totalW := legendW + spacing + (trendBoxW + 20) + spacing + beaconW
	baseX := float64(e.Width) - margin - totalW
	baseY := float64(e.Height) - margin - graphH - 10

	firehoseX := baseX
	firehoseY := baseY
	gx := baseX + legendW + spacing
	gy := baseY

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

	// Calculate interpolated rates based on what's currently at the right edge of the graph
	rows := []legendRow{
		{"PROPAGATION", 0, ColorGossip, ColorGossipUI, func(s MetricSnapshot) int { return s.Gossip }},
		{"PATH CHANGE", 0, ColorUpd, ColorUpdUI, func(s MetricSnapshot) int { return s.Upd }},
		{"WITHDRAWAL", 0, ColorWith, ColorWithUI, func(s MetricSnapshot) int { return s.With }},
		{"NEW PATHS", 0, ColorNew, ColorNewUI, func(s MetricSnapshot) int { return s.New }},
	}
	for i := range rows {
		// Use the stable rate from the previous snapshot (1s ago)
		// to prevent the numbers from spinning/flickering.
		rows[i].val = getRate(hLen-2, rows[i].accessor)
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
	}

	// Draw Trendlines Box
	e.drawTrendlines(screen, gx, gy, graphW, trendBoxW, graphH, fontSize, legendH, rows)
}

func (e *Engine) drawTrendlines(screen *ebiten.Image, gx, gy, graphW, trendBoxW, graphH, fontSize, boxH float64, rows []legendRow) {
	vector.FillRect(screen, float32(gx-10), float32(gy-fontSize-15), float32(trendBoxW+20), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(screen, float32(gx-10), float32(gy-fontSize-15), float32(trendBoxW+20), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

	trendTitle := "ACTIVITY TREND (last minute)"
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

	// Layout constants
	titlePadding := 15.0 // Extra gap from the title
	if e.Width > 2000 {
		titlePadding = 30.0
	}
	chartX := gx
	chartW := graphW + 30.0 // Slightly reduced from 35
	if e.Width > 2000 {
		chartW = graphW + 60.0
	}
	chartH := graphH - titlePadding // Deduct padding from height to keep within box

	// Draw logarithmic grid lines (behind the trend lines)
	for _, val := range []int{1, 10, 100, 1000, 10000, 100000} {
		lVal := logVal(val)
		if lVal > globalMaxLog+0.1 {
			break
		}
		// Round to nearest pixel to prevent shimmering during resize
		y := math.Round(titlePadding + chartH - (lVal/globalMaxLog)*chartH)
		// Darker, thicker, and more stable grid lines
		vector.StrokeLine(screen, float32(chartX), float32(gy+y), float32(gx+chartW), float32(gy+y), 2.0, color.RGBA{40, 40, 40, 255}, true)

		// Draw labels on the right
		labelStr := fmt.Sprintf("%d", val)
		if val >= 1000000 {
			labelStr = fmt.Sprintf("%dm", val/1000000)
		} else if val >= 1000 {
			labelStr = fmt.Sprintf("%dk", val/1000)
		}
		gridFace := &text.GoTextFace{Source: e.fontSource, Size: fontSize * 0.5}
		top := &text.DrawOptions{}
		// Align text to the left of the right margin with closer padding from the chart
		labelX := chartX + chartW + 3 // Slightly reduced from 8
		if e.Width > 2000 {
			labelX = chartX + chartW + 6
		}
		top.GeoM.Translate(labelX, gy+y-(fontSize*0.3))
		top.ColorScale.Scale(1, 1, 1, 0.6)
		text.Draw(screen, labelStr, gridFace, top)
	}

	// Use persistent buffer for trendlines to avoid per-frame allocations
	if e.trendLinesBuffer == nil || e.trendLinesBuffer.Bounds().Dx() != int(chartW) || e.trendLinesBuffer.Bounds().Dy() != int(chartH+10) {
		e.trendLinesBuffer = ebiten.NewImage(int(chartW), int(chartH+10))
	}
	e.trendLinesBuffer.Clear()

	// Draw layers in reverse order of the sorted rows (least active first)
	for i := len(rows) - 1; i >= 0; i-- {
		r := rows[i]
		numSteps := float64(len(e.history) - 2)
		step := chartW / numSteps

		for j := 0; j < len(e.history)-1; j++ {
			x1 := (float64(j) - smoothOffset) * step
			x2 := (float64(j+1) - smoothOffset) * step

			// Fade-out on the left
			alpha := 1.0
			fadeWidth := chartW * 0.1
			if x1 < fadeWidth {
				alpha = x1 / fadeWidth
			}
			if alpha < 0 {
				alpha = 0
			}

			// Shared logarithmic scale
			y1 := chartH - (logVal(r.accessor(e.history[j]))/globalMaxLog)*chartH
			y2 := chartH - (logVal(r.accessor(e.history[j+1]))/globalMaxLog)*chartH

			// Restore vibrant map colors with stable opacity
			c := r.col
			c.A = uint8(255 * alpha * 0.8)

			vector.StrokeLine(e.trendLinesBuffer, float32(x1), float32(y1), float32(x2), float32(y2), 4.0, c, true)
		}
	}

	// Draw the clipped trend image back to the screen
	op := &ebiten.DrawImageOptions{}
	op.GeoM.Translate(chartX, gy+titlePadding)
	screen.DrawImage(e.trendLinesBuffer, op)
}

func (e *Engine) StartMetricsLoop() {
	firstRun := true
	ticker := time.NewTicker(1 * time.Second)
	uiTicks := 0
	lastUIUpdate := time.Now()

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
		targetTicks := 20
		if firstRun {
			targetTicks = 5
		}
		if uiTicks >= targetTicks {
			uiInterval := now.Sub(lastUIUpdate).Seconds()
			lastUIUpdate = now
			firstRun = false
			uiTicks = 0
			e.hubUpdatedAt = now
			e.impactUpdatedAt = now

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
			hubYBase := float64(e.Height) / 2.0

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
					// New hub, fade in
					e.VisualHubs[current[i].cc] = &VisualHub{
						CC:          current[i].cc,
						Rate:        current[i].rate,
						DisplayY:    targetY,
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
				allImpact = append(allImpact, impact{p, float64(count) / uiInterval})
			}
			sort.Slice(allImpact, func(i, j int) bool { return allImpact[i].rate > allImpact[j].rate })

			maxImpact := 5
			if len(allImpact) < maxImpact {
				maxImpact = len(allImpact)
			}

			boxH := 180.0
			if e.Width > 2000 {
				boxH = 360.0
			}
			impactYBase := hubYBase + boxH + 40.0 // Positioned under the hubs box
			if e.Width > 2000 {
				impactYBase = hubYBase + boxH + 80.0
			}

			// Mark all current impact items as inactive
			for _, vi := range e.VisualImpact {
				vi.Active = false
				vi.TargetAlpha = 0.0
			}

			impactSpacing := spacing * 2.4
			for i := 0; i < maxImpact; i++ {
				if allImpact[i].rate < 0.1 && !firstRun {
					continue
				}

				targetY := impactYBase + float64(i)*impactSpacing
				asn := e.prefixToASN[allImpact[i].prefix]
				var networkName string
				if asn != 0 && e.asnMapping != nil {
					networkName = e.asnMapping.GetName(asn)
				}

				if vi, ok := e.VisualImpact[allImpact[i].prefix]; ok {
					vi.Active = true
					vi.TargetY = targetY
					vi.TargetAlpha = 1.0
					vi.Count = allImpact[i].rate
					vi.ASN = asn
					vi.NetworkName = networkName
				} else {
					e.VisualImpact[allImpact[i].prefix] = &VisualImpact{
						Prefix:      allImpact[i].prefix,
						ASN:         asn,
						NetworkName: networkName,
						Count:       allImpact[i].rate,
						DisplayY:    targetY,
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
		}
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
	percentStr := fmt.Sprintf("%.0f%%", e.displayBeaconPercent)
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

	legX := x + (w / 2) - (totalLegW / 2) - 10 // Center relative to donut center (-10 is from centerX calculation)

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

func wrapString(s string, maxChars int, maxLines int) []string {
	if len(s) <= maxChars {
		return []string{s}
	}

	var lines []string
	words := strings.Fields(s)
	currentLine := ""

	for i, word := range words {
		if len(lines) >= maxLines-1 && maxLines > 0 {
			// Last allowed line, append remaining and truncate
			if currentLine != "" {
				remaining := strings.Join(words[i:], " ")
				currentLine += " " + remaining
			} else {
				currentLine = strings.Join(words[i:], " ")
			}

			if len(currentLine) > maxChars {
				currentLine = currentLine[:maxChars-3] + "..."
			}
			lines = append(lines, currentLine)
			return lines
		}

		if currentLine == "" {
			currentLine = word
		} else if len(currentLine)+1+len(word) <= maxChars {
			currentLine += " " + word
		} else {
			lines = append(lines, currentLine)
			currentLine = word
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}

	return lines
}
