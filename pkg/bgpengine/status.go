package bgpengine

import (
	"fmt"
	"image/color"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/biter777/countries"
	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/text/v2"
	"github.com/hajimehoshi/ebiten/v2/vector"
)

type legendRow struct {
	label    string
	val      float64
	col      color.RGBA
	uiCol    color.RGBA
	accessor func(s MetricSnapshot) int
}

func (e *Engine) DrawBGPStatus(screen *ebiten.Image) {
	if e.fontSource == nil {
		return
	}
	margin, fontSize := 40.0, 18.0
	if e.Width > 2000 {
		margin, fontSize = 80.0, 36.0
	}

	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()

	boxW := 280.0
	if e.Width > 2000 {
		boxW = 560.0
	}

	// 1. Left Column: Active Countries & BGP Anomalies
	// Shift down to avoid covering map features
	leftBaselineY := float64(e.Height) * 0.48
	if e.Width > 2000 {
		leftBaselineY = float64(e.Height) * 0.42
	}

	// Active Countries (Top)
	e.drawHubs(screen, margin, leftBaselineY, boxW, fontSize)

	// BGP Anomalies (Below)
	hubsBoxH := 180.0
	if e.Width > 2000 {
		hubsBoxH = 360.0
	}
	impactBoxH := 340.0
	if e.Width > 2000 {
		impactBoxH = 640.0
	}
	impactYBase := leftBaselineY + hubsBoxH + 20.0
	if e.Width > 2000 {
		impactYBase = leftBaselineY + hubsBoxH + 40.0
	}
	e.drawImpacts(screen, margin, impactYBase, boxW, impactBoxH, fontSize, e.titleMonoFace)

	// 3. Bottom Center: Now Playing
	e.drawNowPlaying(screen, margin, boxW, fontSize, e.face)

	// 4. Bottom Right: Legend & Trendlines
	e.drawLegendAndTrends(screen)
}

func (e *Engine) drawHubs(screen *ebiten.Image, margin, hubYBase, boxW, fontSize float64) {
	hubX := margin
	// 1. Check if any items are actually visible
	if len(e.ActiveHubs) == 0 {
		return
	}

	boxH := 180.0
	if e.Width > 2000 {
		boxH = 360.0
	}

	if e.hubsBuffer == nil || e.hubsBuffer.Bounds().Dx() != int(boxW) || e.hubsBuffer.Bounds().Dy() != int(boxH) {
		e.hubsBuffer = ebiten.NewImage(int(boxW), int(boxH))
	}
	e.hubsBuffer.Clear()

	localX, localY := 10.0, fontSize+15.0
	vector.FillRect(e.hubsBuffer, 0, 0, float32(boxW), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(e.hubsBuffer, 0, 0, float32(boxW), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

	hubTitle := "ACTIVE COUNTRIES (ops/s)"
	vector.FillRect(e.hubsBuffer, 0, 0, 4, float32(fontSize+10), ColorNew, false)

	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(localX+5, localY-fontSize-5)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(e.hubsBuffer, hubTitle, e.titleFace, e.textOp)

	for _, vh := range e.ActiveHubs {
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(localX, vh.DisplayY-(hubYBase-localY)+5)
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(vh.Alpha*0.8))
		text.Draw(e.hubsBuffer, vh.CountryStr, e.subMonoFace, e.textOp)

		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(localX+boxW-vh.RateWidth-25, vh.DisplayY-(hubYBase-localY)+5)
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(vh.Alpha*0.6))
		text.Draw(e.hubsBuffer, vh.RateStr, e.subMonoFace, e.textOp)
	}

	now := time.Now()
	isHubUpdating := now.Sub(e.hubUpdatedAt) < 2*time.Second
	hubIntensity := 0.0
	if isHubUpdating {
		hubIntensity = 1.0 - (now.Sub(e.hubUpdatedAt).Seconds() / 2.0)
	}
	e.drawGlitchImage(screen, e.hubsBuffer, hubX-10, hubYBase-fontSize-15, 1.0, hubIntensity, isHubUpdating)
}

func (e *Engine) drawImpacts(screen *ebiten.Image, margin, impactYBase, boxW, impactBoxH, fontSize float64, monoFace *text.GoTextFace) {
	hubX := margin
	// 1. Check if any items are actually visible
	if len(e.ActiveImpacts) == 0 {
		return
	}

	if e.impactBuffer == nil || e.impactBuffer.Bounds().Dx() != int(boxW) || e.impactBuffer.Bounds().Dy() != int(impactBoxH) {
		e.impactBuffer = ebiten.NewImage(int(boxW), int(impactBoxH))
	}
	e.impactBuffer.Clear()

	localX, localY := 10.0, fontSize+15.0
	vector.FillRect(e.impactBuffer, 0, 0, float32(boxW), float32(impactBoxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(e.impactBuffer, 0, 0, float32(boxW), float32(impactBoxH), 1, color.RGBA{36, 42, 53, 255}, false)

	impactTitle := "BGP ANOMALIES (last 10 mins)"
	vector.FillRect(e.impactBuffer, 0, 0, 4, float32(fontSize+10), ColorNew, false)

	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(localX+5, localY-fontSize-5)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(e.impactBuffer, impactTitle, e.titleFace, e.textOp)

	for _, v := range e.ActiveImpacts {
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(localX, v.DisplayY-(impactYBase-localY)+8)
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(v.Alpha*0.8))
		text.Draw(e.impactBuffer, v.Prefix, monoFace, e.textOp)

		// Draw classification name on the right in its specific color
		if v.DisplayClassificationName != "" {
			tw, _ := text.Measure(v.DisplayClassificationName, e.subMonoFace, 0)
			e.textOp.GeoM.Reset()
			e.textOp.GeoM.Translate(localX+boxW-tw-25, v.DisplayY-(impactYBase-localY)+8+fontSize*0.1)
			e.textOp.ColorScale.Reset()
			cr, cg, cb := float32(v.DisplayClassificationColor.R)/255.0, float32(v.DisplayClassificationColor.G)/255.0, float32(v.DisplayClassificationColor.B)/255.0
			e.textOp.ColorScale.Scale(cr, cg, cb, float32(v.Alpha*0.9))
			text.Draw(e.impactBuffer, v.DisplayClassificationName, e.subMonoFace, e.textOp)
		}

		if v.asnStr != "" {
			for j, line := range v.asnLines {
				e.textOp.GeoM.Reset()
				e.textOp.GeoM.Translate(localX, v.DisplayY-(impactYBase-localY)+8+fontSize*1.2+float64(j)*e.subMonoFace.Size*1.1)
				e.textOp.ColorScale.Reset()
				e.textOp.ColorScale.Scale(1, 1, 1, float32(v.Alpha*0.4))
				text.Draw(e.impactBuffer, line, e.subMonoFace, e.textOp)
			}
		}
	}

	if e.orangeCount > 0 || e.redCount > 0 {
		label := "Prefix Counts: "
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(localX, impactBoxH-fontSize-15)
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
		text.Draw(e.impactBuffer, label, e.subMonoFace, e.textOp)
		lw, _ := text.Measure(label, e.subMonoFace, 0)

		currentX := localX + lw
		hasBad := e.orangeCount > 0
		if hasBad {
			badStr := fmt.Sprintf("%d bad", e.orangeCount)
			e.textOp.GeoM.Reset()
			e.textOp.GeoM.Translate(currentX, impactBoxH-fontSize-15)
			e.textOp.ColorScale.Reset()
			e.textOp.ColorScale.ScaleWithColor(ColorBad)
			text.Draw(e.impactBuffer, badStr, e.subMonoFace, e.textOp)
			bw, _ := text.Measure(badStr, e.subMonoFace, 0)
			currentX += bw
		}

		if e.redCount > 0 {
			if hasBad {
				comma := ", "
				e.textOp.GeoM.Reset()
				e.textOp.GeoM.Translate(currentX, impactBoxH-fontSize-15)
				e.textOp.ColorScale.Reset()
				e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
				text.Draw(e.impactBuffer, comma, e.subMonoFace, e.textOp)
				cw, _ := text.Measure(comma, e.subMonoFace, 0)
				currentX += cw
			}

			critStr := fmt.Sprintf("%d critical", e.redCount)
			e.textOp.GeoM.Reset()
			e.textOp.GeoM.Translate(currentX, impactBoxH-fontSize-15)
			e.textOp.ColorScale.Reset()
			e.textOp.ColorScale.ScaleWithColor(ColorCritical)
			text.Draw(e.impactBuffer, critStr, e.subMonoFace, e.textOp)
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

func (e *Engine) drawNowPlaying(screen *ebiten.Image, margin, boxW, fontSize float64, face *text.GoTextFace) {
	now := time.Now()
	if e.CurrentSong == "" {
		return
	}
	songX := float64(e.Width) - margin - (boxW * 1.0)
	songYBase := margin + fontSize + 15
	songBoxW := boxW * 1.0
	boxHSong := fontSize * 2.5
	if e.CurrentArtist != "" {
		boxHSong += fontSize * 1.2
	}
	if e.CurrentExtra != "" {
		boxHSong += fontSize * 1.2
	}

	if e.nowPlayingBuffer == nil || e.nowPlayingBuffer.Bounds().Dx() != int(songBoxW) || e.nowPlayingBuffer.Bounds().Dy() != int(boxHSong) {
		e.nowPlayingBuffer = ebiten.NewImage(int(songBoxW), int(boxHSong))
	}
	e.nowPlayingBuffer.Clear()

	localX, localY := 10.0, fontSize+15.0
	vector.FillRect(e.nowPlayingBuffer, 0, 0, float32(songBoxW), float32(boxHSong), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(e.nowPlayingBuffer, 0, 0, float32(songBoxW), float32(boxHSong), 1, color.RGBA{36, 42, 53, 255}, false)

	songTitle := "NOW PLAYING"
	vector.FillRect(e.nowPlayingBuffer, 0, 0, 4, float32(fontSize+10), ColorNew, false)

	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(localX+5, localY-fontSize-5)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(e.nowPlayingBuffer, songTitle, e.titleFace, e.textOp)

	isGlitching := now.Sub(e.songChangedAt) < 2*time.Second
	intensity := 0.0
	if isGlitching {
		intensity = 1.0 - (now.Sub(e.songChangedAt).Seconds() / 2.0)
	}

	e.drawMarquee(e.nowPlayingBuffer, e.CurrentSong, face, localX, localY+fontSize*0.2, 0.8, isGlitching, intensity, &e.songBuffer)

	yOffset := fontSize * 1.1
	if e.CurrentArtist != "" {
		e.drawMarquee(e.nowPlayingBuffer, e.CurrentArtist, e.artistFace, localX, localY+yOffset, 0.5, isGlitching, intensity, &e.artistBuffer)
		yOffset += fontSize * 1.1
	}

	if e.CurrentExtra != "" {
		e.drawMarquee(e.nowPlayingBuffer, e.CurrentExtra, e.extraFace, localX, localY+yOffset, 0.4, isGlitching, intensity, &e.extraBuffer)
	}

	e.drawGlitchImage(screen, e.nowPlayingBuffer, songX-10, songYBase-fontSize-15, 1.0, intensity, isGlitching)
}

func (e *Engine) drawLegendAndTrends(screen *ebiten.Image) {
	margin, fontSize := 40.0, 18.0
	if e.Width > 2000 {
		margin, fontSize = 80.0, 36.0
	}

	// 3. Bottom Right: Legend & Trendlines
	var graphW, legendW, legendH float64
	if e.Width > 2000 {
		graphW = 600.0
		legendW, legendH = 1300.0, 300.0
	} else {
		graphW = 300.0
		legendW, legendH = 900.0, 150.0
	}

	// Match heights
	trendBoxH := legendH - fontSize - 25 // Calculate inner graph height area to match legend box height
	graphH := trendBoxH - 10             // Leave some room inside

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

	// Draw subtle hacker-green accent
	vector.FillRect(screen, float32(firehoseX-10), float32(firehoseY-fontSize-15), 4, float32(fontSize+10), ColorNew, false)

	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(firehoseX+5, firehoseY-fontSize-5)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(screen, legendTitle, e.titleFace, e.textOp)

	swatchSize := fontSize

	// Shift legend content slightly for padding
	firehoseX += 10
	firehoseY += 10

	hLen := len(e.history)
	getRate := func(idx int, getValue func(s MetricSnapshot) int) float64 {
		if hLen < 2 || idx < 0 || idx >= hLen {
			return 0
		}
		// Snaps were 1s apart, so interval is 1.0
		return float64(getValue(e.history[idx])) / 1.0
	}

	// Calculate interpolated rates based on what's currently at the right edge of the graph
	for i := range e.legendRows {
		// Use the stable rate from the previous snapshot (1s ago)
		// to prevent the numbers from spinning/flickering.
		e.legendRows[i].val = getRate(hLen-2, e.legendRows[i].accessor)
	}

	// Draw Legend columns
	colWidth := (legendW - 60) / 3
	for i, r := range e.legendRows {
		var col, row int
		switch {
		case i < 4: // Normal (Discovery, Churn, Hunting, Oscill)
			col = 0
			row = i
		case i < 8: // Bad (Flaps, Babbling, NH, Agg)
			col = 1
			row = i - 4
		default: // Critical (Leak, Outage)
			col = 2
			row = i - 8
		}

		x := firehoseX + float64(col)*colWidth
		y := firehoseY + float64(row)*(fontSize+10)

		// Draw the pulse circle (swatch) - using the map color (col)
		cr, cg, cb := float32(r.col.R)/255.0, float32(r.col.G)/255.0, float32(r.col.B)/255.0
		baseAlpha := float32(0.6)
		if r.col == ColorDiscovery {
			baseAlpha = 0.4
		}

		imgToDraw := e.pulseImage
		imgWidth := float64(e.pulseImage.Bounds().Dx())
		halfWidth := imgWidth / 2
		if r.label == "ROUTE LEAK" {
			imgToDraw = e.flareImage
			imgWidth = float64(e.flareImage.Bounds().Dx())
			halfWidth = imgWidth / 2
			baseAlpha = 1.0 // Make it fully visible in legend
		}

		e.drawOp.GeoM.Reset()
		e.drawOp.Blend = ebiten.BlendLighter
		scale := swatchSize / imgWidth * 1
		e.drawOp.GeoM.Translate(-halfWidth, -halfWidth)
		e.drawOp.GeoM.Scale(scale, scale)
		e.drawOp.GeoM.Translate(x+(swatchSize/2), y+(fontSize/2))
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.Scale(cr*baseAlpha, cg*baseAlpha, cb*baseAlpha, baseAlpha)
		screen.DrawImage(imgToDraw, e.drawOp)

		// Draw the text label in the legend box
		tr, tg, tb := float32(r.uiCol.R)/255.0, float32(r.uiCol.G)/255.0, float32(r.uiCol.B)/255.0
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(x+swatchSize+15, y)
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(tr, tg, tb, 0.9)
		text.Draw(screen, r.label, e.face, e.textOp)
	}

	// Draw Trendlines Box
	e.drawTrendlines(screen, gx, gy, graphW, trendBoxW, graphH, fontSize, legendH)
}

func (e *Engine) drawTrendlines(screen *ebiten.Image, gx, gy, graphW, trendBoxW, graphH, fontSize, boxH float64) {
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

	titlePadding := 15.0 // Extra gap from the title
	if e.Width > 2000 {
		titlePadding = 30.0
	}
	chartW := graphW + 30.0
	if e.Width > 2000 {
		chartW = graphW + 60.0
	}
	chartH := graphH - titlePadding

	globalMaxLog := e.calculateGlobalMaxLog()
	e.drawTrendGrid(screen, gx, gy, chartW, chartH, titlePadding, globalMaxLog, fontSize)

	// Use persistent buffer for trendlines to avoid per-frame allocations
	if e.trendLinesBuffer == nil || e.trendLinesBuffer.Bounds().Dx() != int(chartW) || e.trendLinesBuffer.Bounds().Dy() != int(chartH+10) {
		e.trendLinesBuffer = ebiten.NewImage(int(chartW), int(chartH+10))
	}
	e.trendLinesBuffer.Clear()

	e.drawTrendLayers(chartW, chartH, globalMaxLog)

	// Draw the clipped trend image back to the screen
	e.drawOp.GeoM.Reset()
	e.drawOp.GeoM.Translate(gx, gy+titlePadding)
	e.drawOp.ColorScale.Reset()
	e.drawOp.ColorScale.Scale(1, 1, 1, 0.8) // Apply transparency globally
	screen.DrawImage(e.trendLinesBuffer, e.drawOp)
}

func (e *Engine) calculateGlobalMaxLog() float64 {
	globalMaxLog := 1.0
	for i := range e.history {
		good, poly, bad, crit := e.aggregateMetrics(&e.history[i])
		for _, v := range []int{good, poly, bad, crit} {
			if l := e.logVal(v); l > globalMaxLog {
				globalMaxLog = l
			}
		}
	}
	return globalMaxLog
}

func (e *Engine) aggregateMetrics(s *MetricSnapshot) (good, poly, bad, crit int) {
	// Normal (Blue)
	good = s.Global
	// Policy (Purple)
	poly = s.Hunting + s.TE + s.Oscill
	// Bad (Orange)
	bad = s.LinkFlap + s.AggFlap + s.Babbling + s.NextHop
	// Critical (Red)
	crit = s.Outage + s.Leak
	return
}

func (e *Engine) logVal(v int) float64 {
	if v <= 0 {
		return 0
	}
	return math.Log10(float64(v) + 1.0)
}

func (e *Engine) drawTrendGrid(screen *ebiten.Image, gx, gy, chartW, chartH, titlePadding, globalMaxLog, fontSize float64) {
	for _, val := range []int{1, 10, 100, 1000, 10000, 100000} {
		lVal := e.logVal(val)
		if lVal > globalMaxLog+0.1 {
			break
		}
		y := math.Round(titlePadding + chartH - (lVal/globalMaxLog)*chartH)
		vector.StrokeLine(screen, float32(gx), float32(gy+y), float32(gx+chartW), float32(gy+y), 2.0, color.RGBA{40, 40, 40, 255}, true)

		labelStr := fmt.Sprintf("%d", val)
		if val >= 1000000 {
			labelStr = fmt.Sprintf("%dm", val/1000000)
		} else if val >= 1000 {
			labelStr = fmt.Sprintf("%dk", val/1000)
		}
		e.textOp.GeoM.Reset()
		labelX := gx + chartW + 8
		if e.Width > 2000 {
			labelX = gx + chartW + 16
		}
		e.textOp.GeoM.Translate(labelX, gy+y-(fontSize*0.3))
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, 0.6)
		text.Draw(screen, labelStr, e.titleFace05, e.textOp)
	}
}

func (e *Engine) drawTrendLayers(chartW, chartH, globalMaxLog float64) {
	smoothOffset := 1.0
	if time.Since(e.lastMetricsUpdate) <= 1*time.Second {
		smoothOffset = math.Mod(time.Since(e.lastMetricsUpdate).Seconds(), 1.0)
	}

	hLen := len(e.history)
	numSteps := float64(hLen - 2)
	if numSteps <= 0 {
		numSteps = 1
	}
	step := chartW / numSteps

	// Colors for the four aggregated lines
	goodCol := ColorDiscovery // Blue (Normal)
	polyCol := ColorPolicy    // Purple (Policy)
	badCol := ColorBad        // Orange (Bad)
	critCol := ColorCritical  // Red (Critical)

	e.drawOp.Blend = ebiten.BlendLighter

	// Helper to draw a line segment
	drawLine := func(val1, val2 int, c color.RGBA, j int) {
		if val1 == 0 && val2 == 0 {
			return
		}
		x1 := (float64(j) - smoothOffset) * step
		x2 := (float64(j+1) - smoothOffset) * step
		y1 := chartH - (e.logVal(val1)/globalMaxLog)*chartH
		y2 := chartH - (e.logVal(val2)/globalMaxLog)*chartH

		dx := x2 - x1
		dy := y2 - y1
		length := math.Hypot(dx, dy)
		angle := math.Atan2(dy, dx)
		thickness := 4.0

		e.drawOp.GeoM.Reset()
		e.drawOp.GeoM.Translate(0, -0.5)
		e.drawOp.GeoM.Scale(length, thickness)
		e.drawOp.GeoM.Rotate(angle)
		e.drawOp.GeoM.Translate(x1, y1)
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.ScaleWithColor(c)
		e.trendLinesBuffer.DrawImage(e.trendLineImg, e.drawOp)
	}

	for j := 0; j < hLen-1; j++ {
		g1, p1, b1, c1 := e.aggregateMetrics(&e.history[j])
		g2, p2, b2, c2 := e.aggregateMetrics(&e.history[j+1])

		// Draw lines in order from bottom to top (Good -> Policy -> Bad -> Crit)
		drawLine(g1, g2, goodCol, j)
		drawLine(p1, p2, polyCol, j)
		drawLine(b1, b2, badCol, j)
		drawLine(c1, c2, critCol, j)
	}
}

func (e *Engine) StartMetricsLoop() {
	ticker := time.NewTicker(1 * time.Second)
	uiTicks := 0
	lastUIUpdate := time.Now()
	firstRun := true

	run := func() {
		e.metricsMu.Lock()
		defer e.metricsMu.Unlock()

		now := time.Now()
		interval := now.Sub(e.lastMetricsUpdate).Seconds()
		if interval <= 0 {
			interval = 1.0
		}
		e.lastMetricsUpdate = now

		e.updateMetricSnapshots(interval)

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

			e.updateVisualHubs(uiInterval, firstRun)
			e.updateVisualImpacts(uiInterval)

			e.prefixImpactHistory = append(e.prefixImpactHistory[1:], make(map[string]int))
			e.currentAnomalies = make(map[Level2EventType]map[string]int)
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

func (e *Engine) updateMetricSnapshots(interval float64) {
	snap := MetricSnapshot{
		New:    int(e.windowNew),
		Upd:    int(e.windowUpd),
		With:   int(e.windowWith),
		Gossip: int(e.windowGossip),
		Note:   int(e.windowNote),
		Peer:   int(e.windowPeer),
		Open:   int(e.windowOpen),
		Beacon: int(e.windowBeacon),

		LinkFlap: int(e.windowLinkFlap),
		AggFlap:  int(e.windowAggFlap),
		Oscill:   int(e.windowOscill),
		Babbling: int(e.windowBabbling),
		Hunting:  int(e.windowHunting),
		TE:       int(e.windowTE),
		NextHop:  int(e.windowNextHop),
		Outage:   int(e.windowOutage),
		Leak:     int(e.windowLeak),
		Global:   int(e.windowGlobal),
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

	e.windowLinkFlap, e.windowAggFlap, e.windowOscill, e.windowBabbling = 0, 0, 0, 0
	e.windowHunting, e.windowTE, e.windowNextHop, e.windowOutage = 0, 0, 0, 0
	e.windowLeak, e.windowGlobal = 0, 0
}

func (e *Engine) updateVisualHubs(uiInterval float64, firstRun bool) {
	current := e.getSortedHubs(uiInterval)
	maxItems := 7
	if len(current) < maxItems {
		maxItems = len(current)
	}

	fontSize := 18.0
	hubYBase := float64(e.Height) * 0.48
	if e.Width > 2000 {
		fontSize = 36.0
		hubYBase = float64(e.Height) * 0.42
	}
	spacing := fontSize * 1.0

	for _, vh := range e.VisualHubs {
		vh.Active = false
		vh.TargetAlpha = 0.0
	}

	e.ActiveHubs = nil
	for i := 0; i < maxItems; i++ {
		if current[i].rate < 0.1 && !firstRun {
			continue
		}
		targetY := hubYBase + float64(i)*spacing
		vh := e.getOrCreateVisualHub(current[i].cc, targetY)

		vh.Active = true
		vh.TargetY = targetY
		if vh.Alpha < 0.01 {
			vh.DisplayY = vh.TargetY
		}
		vh.TargetAlpha = 1.0
		vh.Rate = current[i].rate
		vh.RateStr = fmt.Sprintf("%.0f", current[i].rate)
		vh.RateWidth, _ = text.Measure(vh.RateStr, e.subMonoFace, 0)
		e.ActiveHubs = append(e.ActiveHubs, vh)
	}

	for cc, vh := range e.VisualHubs {
		if !vh.Active {
			delete(e.VisualHubs, cc)
		}
	}
}

type hub struct {
	cc   string
	rate float64
}

func (e *Engine) getSortedHubs(uiInterval float64) []hub {
	var current []hub
	for cc, val := range e.countryActivity {
		current = append(current, hub{cc, float64(val) / uiInterval})
	}
	sort.Slice(current, func(i, j int) bool { return current[i].rate > current[j].rate })
	return current
}

func (e *Engine) getOrCreateVisualHub(cc string, targetY float64) *VisualHub {
	vh, ok := e.VisualHubs[cc]
	if !ok {
		countryName := countries.ByName(cc).String()
		if countryName == "Unknown" {
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

		vh = &VisualHub{
			CC:         cc,
			CountryStr: countryName,
			DisplayY:   targetY,
			Alpha:      0,
		}
		e.VisualHubs[cc] = vh
	}
	return vh
}

func (e *Engine) updateVisualImpacts(uiInterval float64) {
	allImpact := e.gatherActiveImpacts(uiInterval)

	sort.SliceStable(allImpact, func(i, j int) bool {
		p1, p2 := e.GetPriority(allImpact[i].ClassificationName), e.GetPriority(allImpact[j].ClassificationName)
		if p1 != p2 {
			return p1 > p2 // Prioritize Critical (Red) over Bad (Orange)
		}

		// Prioritize by network size (less specific/smaller mask first, e.g. /16 > /24)
		m1, m2 := allImpact[i].MaskLen, allImpact[j].MaskLen
		if m1 != m2 {
			return m1 < m2
		}

		// Use count as a final tie-breaker for same-sized prefixes
		return allImpact[i].Count > allImpact[j].Count
	})

	for _, v := range e.VisualImpact {
		v.Active = false
		v.TargetAlpha = 0.0
	}

	e.activateTopImpacts(allImpact)

	for p, v := range e.VisualImpact {
		if !v.Active && v.Alpha < 0.01 {
			delete(e.VisualImpact, p)
		}
	}
}

func getMaskLen(prefix string) int {
	idx := strings.IndexByte(prefix, '/')
	if idx == -1 {
		return 0
	}
	mask, _ := strconv.Atoi(prefix[idx+1:])
	return mask
}

func (e *Engine) gatherActiveImpacts(uiInterval float64) []*VisualImpact {
	allImpact := make([]*VisualImpact, 0)
	for et, prefixes := range e.currentAnomalies {
		prio := e.GetPriority(et.String())
		if prio < 2 {
			continue
		}

		for p, count := range prefixes {
			v, ok := e.VisualImpact[p]
			if !ok {
				v = &VisualImpact{Prefix: p, MaskLen: getMaskLen(p)}
				e.VisualImpact[p] = v
			}

			// Store the anomaly categorization
			v.ClassificationName = et.String()
			v.ClassificationColor, _ = e.getLevel2Visuals(et)

			v.Count = float64(count) / uiInterval
			allImpact = append(allImpact, v)
		}
	}
	return allImpact
}

func (e *Engine) activateTopImpacts(allImpact []*VisualImpact) {
	var orangeCount, redCount int
	for _, vi := range allImpact {
		prio := e.GetPriority(vi.ClassificationName)
		switch prio {
		case 2:
			orangeCount++
		case 3:
			redCount++
		}
	}

	e.orangeCount = orangeCount
	e.redCount = redCount

	fontSize := 18.0
	hubsBoxH, boxW := 180.0, 280.0
	if e.Width > 2000 {
		fontSize = 36.0
		hubsBoxH, boxW = 360.0, 560.0
	}
	spacing := fontSize * 1.2
	hubYBase := float64(e.Height) * 0.48
	if e.Width > 2000 {
		hubYBase = float64(e.Height) * 0.42
	}
	impactYBase := hubYBase + hubsBoxH + 20.0
	if e.Width > 2000 {
		impactYBase = hubYBase + hubsBoxH + 40.0
	}

	maxImpact := 5
	if len(allImpact) < maxImpact {
		maxImpact = len(allImpact)
	}
	impactSpacing := spacing * 2.4
	e.ActiveImpacts = nil
	for i := 0; i < maxImpact; i++ {
		v := allImpact[i]
		v.DisplayClassificationName = v.ClassificationName
		v.DisplayClassificationColor = v.ClassificationColor

		asn := e.prefixToASN[v.Prefix]
		var networkName string
		if asn != 0 && e.asnMapping != nil {
			networkName = e.asnMapping.GetName(asn)
		}

		asnStr := ""
		if asn != 0 {
			asnStr = fmt.Sprintf("AS%d", asn)
			if networkName != "" {
				asnStr = fmt.Sprintf("AS%d - %s", asn, networkName)
			}
		}

		charW, _ := text.Measure("A", e.subMonoFace, 0)
		maxChars := int((boxW - 100) / charW)
		if maxChars < 10 {
			maxChars = 10
		}

		v.Active = true
		v.TargetY = impactYBase + float64(i)*impactSpacing
		if v.Alpha < 0.01 {
			v.DisplayY = v.TargetY
		}
		v.TargetAlpha = 1.0
		v.ASN = asn
		v.NetworkName = networkName
		v.asnStr = asnStr
		v.asnLines = wrapString(asnStr, maxChars, 2)
		v.RateStr = fmt.Sprintf("%.1f", v.Count)
		v.RateWidth, _ = text.Measure(v.RateStr, e.titleMonoFace, 0)
		e.ActiveImpacts = append(e.ActiveImpacts, v)
	}
}

func (e *Engine) drawBeaconMetrics(screen *ebiten.Image, x, y, w, h, fontSize, boxH float64) {
	vector.FillRect(screen, float32(x-10), float32(y-fontSize-15), float32(w), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(screen, float32(x-10), float32(y-fontSize-15), float32(w), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

	title := "BEACON ANALYSIS"
	vector.FillRect(screen, float32(x-10), float32(y-fontSize-15), 4, float32(fontSize+10), color.RGBA{255, 165, 0, 255}, false) // Orange accent

	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(x+5, y-fontSize-5)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(screen, title, e.titleFace, e.textOp)

	// Donut Pie Chart dimensions
	radius := h * 0.38
	centerX := x + (w / 2) - 10
	centerY := y + (h / 2) - 10

	// 1. Background circle (Organic traffic color)
	organicCol := color.RGBA{100, 100, 100, 255}
	var bgPath vector.Path
	bgPath.Arc(float32(centerX), float32(centerY), float32(radius), 0, 2*math.Pi, vector.Clockwise)
	e.vectorDrawPathOp.ColorScale.Reset()
	e.vectorDrawPathOp.ColorScale.ScaleWithColor(organicCol)
	vector.FillPath(screen, &bgPath, &e.vectorFillOp, &e.vectorDrawPathOp)

	// 2. Beacon slice
	if e.displayBeaconPercent > 0.01 {
		var beaconPath vector.Path
		startAngle := -math.Pi / 2 // Top
		endAngle := startAngle + (2 * math.Pi * e.displayBeaconPercent / 100.0)
		beaconPath.MoveTo(float32(centerX), float32(centerY))
		beaconPath.Arc(float32(centerX), float32(centerY), float32(radius), float32(startAngle), float32(endAngle), vector.Clockwise)
		beaconPath.LineTo(float32(centerX), float32(centerY))
		e.vectorDrawPathOp.ColorScale.Reset()
		e.vectorDrawPathOp.ColorScale.ScaleWithColor(color.RGBA{255, 165, 0, 255})
		vector.FillPath(screen, &beaconPath, &e.vectorFillOp, &e.vectorDrawPathOp)
	}

	// 3. Center cutout (Donut)
	var holePath vector.Path
	holePath.Arc(float32(centerX), float32(centerY), float32(radius*0.6), 0, 2*math.Pi, vector.Clockwise)
	e.vectorDrawPathOp.ColorScale.Reset()
	e.vectorDrawPathOp.ColorScale.ScaleWithColor(color.RGBA{15, 15, 15, 255})
	vector.FillPath(screen, &holePath, &e.vectorFillOp, &e.vectorDrawPathOp)

	// 4. Text Label in Center
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.8)
	label := fmt.Sprintf("%.0f%%", e.displayBeaconPercent)
	tw, th := text.Measure(label, e.titleMonoFace, 0)
	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(centerX-(tw/2), centerY-(th/2))
	text.Draw(screen, label, e.titleMonoFace, e.textOp)

	// 5. Small Legend Items below chart
	legendY := y + h - fontSize*0.8
	e.drawBeaconLegendItem(screen, x, legendY, fontSize, color.RGBA{255, 165, 0, 255}, "Beacon")
	e.drawBeaconLegendItem(screen, x+(w/2), legendY, fontSize, organicCol, "Organic")
}

func (e *Engine) drawBeaconLegendItem(screen *ebiten.Image, x, y, fontSize float64, c color.RGBA, label string) {
	swatchSize := fontSize * 0.6
	_, th := text.Measure(label, e.subFace, 0)

	vector.FillRect(screen, float32(x), float32(y+(fontSize-swatchSize)/2), float32(swatchSize), float32(swatchSize), c, false)
	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(x+swatchSize+10, y+(fontSize-th)/2)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.6)
	text.Draw(screen, label, e.subFace, e.textOp)
}

func (e *Engine) drawMarquee(dst *ebiten.Image, content string, face *text.GoTextFace, x, y, alpha float64, isGlitching bool, intensity float64, buffer **ebiten.Image) {
	if content == "" {
		return
	}
	tw, th := text.Measure(content, face, 0)
	if *buffer == nil || (*buffer).Bounds().Dx() != int(tw+50) {
		*buffer = ebiten.NewImage(int(tw+50), int(th+10))
	}
	(*buffer).Clear()
	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(0, 0)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 1.0)
	text.Draw(*buffer, content, face, e.textOp)

	// Draw to destination
	e.drawOp.GeoM.Reset()
	e.drawOp.GeoM.Translate(x, y)
	e.drawOp.ColorScale.Reset()
	e.drawOp.ColorScale.Scale(1, 1, 1, float32(alpha))
	dst.DrawImage(*buffer, e.drawOp)

	if isGlitching && rand.Float64() < intensity*0.5 {
		offset := 2.0 * intensity
		e.drawOp.GeoM.Reset()
		e.drawOp.GeoM.Translate(x+offset, y)
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.Scale(1, 0, 0, float32(alpha*0.5*intensity))
		dst.DrawImage(*buffer, e.drawOp)

		e.drawOp.GeoM.Reset()
		e.drawOp.GeoM.Translate(x-offset, y)
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.Scale(0, 1, 1, float32(alpha*0.5*intensity))
		dst.DrawImage(*buffer, e.drawOp)
	}
}

func wrapString(s string, maxChars, maxLines int) []string {
	if s == "" || maxChars <= 0 {
		return nil
	}
	words := strings.Fields(s)
	if len(words) == 0 {
		return []string{s}
	}

	var lines []string
	currentLine := ""

	for i := 0; i < len(words); i++ {
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

		testLine := currentLine
		if testLine != "" {
			testLine += " "
		}
		testLine += words[i]

		if len(testLine) > maxChars {
			if currentLine != "" {
				lines = append(lines, currentLine)
				currentLine = words[i]
			} else {
				// Single word is too long
				lines = append(lines, words[i][:maxChars-3]+"...")
				currentLine = ""
			}
		} else {
			currentLine = testLine
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}
	return lines
}
