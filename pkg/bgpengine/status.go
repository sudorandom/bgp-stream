// Package bgpengine provides the core logic for the BGP stream engine, including status visualization.
package bgpengine

import (
	"fmt"
	"image"
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

func (e *Engine) drawMarquee(screen *ebiten.Image, label string, f *text.GoTextFace, tx, ty, songBoxW, alpha float64, isGlitching bool, intensity float64, buffer **ebiten.Image) {
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
		}
		(*buffer).Clear()

		// Draw text with offset
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(-offset, 0)
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(alpha))
		text.Draw(*buffer, label, f, e.textOp)

		// Draw second copy for seamless loop
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(totalW-offset, 0)
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(alpha))
		text.Draw(*buffer, label, f, e.textOp)

		// Draw clipped result to buffer
		e.drawOp.GeoM.Reset()
		e.drawOp.GeoM.Translate(tx, ty)
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.Scale(1, 1, 1, 1)
		e.drawOp.Blend = ebiten.BlendSourceOver
		screen.DrawImage(*buffer, e.drawOp)
	} else {
		if isGlitching {
			// Draw glitched text into buffer
			e.drawGlitchTextSubtle(screen, label, f, tx, ty, float32(alpha), intensity, true)
		} else {
			e.textOp.GeoM.Reset()
			e.textOp.GeoM.Translate(tx, ty)
			e.textOp.ColorScale.Reset()
			e.textOp.ColorScale.Scale(1, 1, 1, float32(alpha))
			text.Draw(screen, label, f, e.textOp)
		}
	}
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

	boxW, _, _ := e.getMetricBoxDimensions()
	if e.MinimalUI {
		e.drawNowPlaying(screen, margin, boxW, fontSize, e.face)
		e.metricsMu.Unlock()
		return
	}

	hubYBase := float64(e.Height) / 2.0
	_, boxH, impactBoxH := e.getMetricBoxDimensions()

	e.drawHubs(screen, margin, hubYBase, boxW, boxH, fontSize, e.monoFace)
	impactYBase := hubYBase + boxH + 40.0
	if e.Width > 2000 {
		impactYBase = hubYBase + boxH + 80.0
	}
	e.drawImpacts(screen, margin, impactYBase, boxW, impactBoxH, fontSize, e.monoFace)
	e.drawNowPlaying(screen, margin, boxW, fontSize, e.face)

	e.metricsMu.Unlock()
	e.DrawBGPStatus(screen)
}

func (e *Engine) getMetricBoxDimensions() (boxW, boxH, impactBoxH float64) {
	boxW, boxH, impactBoxH = 280.0, 180.0, 320.0
	if e.Width > 2000 {
		boxW, boxH, impactBoxH = 560.0, 360.0, 640.0
	}
	return boxW, boxH, impactBoxH
}

func (e *Engine) drawHubs(screen *ebiten.Image, margin, hubYBase, boxW, boxH, fontSize float64, monoFace *text.GoTextFace) {
	hubX := margin
	if len(e.VisualHubs) == 0 {
		return
	}
	if e.hubsBuffer == nil || e.hubsBuffer.Bounds().Dx() != int(boxW) || e.hubsBuffer.Bounds().Dy() != int(boxH) {
		e.hubsBuffer = ebiten.NewImage(int(boxW), int(boxH))
	}
	e.hubsBuffer.Clear()

	localX, localY := 10.0, fontSize+15.0
	vector.FillRect(e.hubsBuffer, 0, 0, float32(boxW), float32(boxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(e.hubsBuffer, 0, 0, float32(boxW), float32(boxH), 1, color.RGBA{36, 42, 53, 255}, false)

	titleLabel := "TOP ACTIVITY HUBS (ops/s)"
	vector.FillRect(e.hubsBuffer, 0, 0, 4, float32(fontSize+10), ColorNew, false)

	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(localX+5, localY-fontSize-5)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(e.hubsBuffer, titleLabel, e.titleFace, e.textOp)

	for _, vh := range e.VisualHubs {
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(localX, vh.DisplayY-(hubYBase-localY))
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(vh.Alpha*0.8))
		text.Draw(e.hubsBuffer, vh.CountryStr, monoFace, e.textOp)

		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(localX+boxW-vh.RateWidth-25, vh.DisplayY-(hubYBase-localY))
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(vh.Alpha*0.6))
		text.Draw(e.hubsBuffer, vh.RateStr, monoFace, e.textOp)
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
	if len(e.VisualImpact) == 0 {
		return
	}
	if e.impactBuffer == nil || e.impactBuffer.Bounds().Dx() != int(boxW) || e.impactBuffer.Bounds().Dy() != int(impactBoxH) {
		e.impactBuffer = ebiten.NewImage(int(boxW), int(impactBoxH))
	}
	e.impactBuffer.Clear()

	localX, localY := 10.0, fontSize+15.0
	vector.FillRect(e.impactBuffer, 0, 0, float32(boxW), float32(impactBoxH), color.RGBA{0, 0, 0, 100}, false)
	vector.StrokeRect(e.impactBuffer, 0, 0, float32(boxW), float32(impactBoxH), 1, color.RGBA{36, 42, 53, 255}, false)

	impactTitle := "MOST ACTIVE PREFIXES (ops/s)"
	vector.FillRect(e.impactBuffer, 0, 0, 4, float32(fontSize+10), ColorNew, false)

	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(localX+5, localY-fontSize-5)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(e.impactBuffer, impactTitle, e.titleFace, e.textOp)

	for _, vi := range e.VisualImpact {
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(localX, vi.DisplayY-(impactYBase-localY))
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(vi.Alpha*0.8))
		text.Draw(e.impactBuffer, vi.Prefix, monoFace, e.textOp)

		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(localX+boxW-vi.RateWidth-25, vi.DisplayY-(impactYBase-localY))
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(1, 1, 1, float32(vi.Alpha*0.6))
		text.Draw(e.impactBuffer, vi.RateStr, monoFace, e.textOp)

		if vi.asnStr != "" {
			for j, line := range vi.asnLines {
				e.textOp.GeoM.Reset()
				e.textOp.GeoM.Translate(localX, vi.DisplayY-(impactYBase-localY)+fontSize*1.2+float64(j)*e.subMonoFace.Size*1.1)
				e.textOp.ColorScale.Reset()
				e.textOp.ColorScale.Scale(1, 1, 1, float32(vi.Alpha*0.4))
				text.Draw(e.impactBuffer, line, e.subMonoFace, e.textOp)
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

func (e *Engine) drawNowPlaying(screen *ebiten.Image, margin, boxW, fontSize float64, face *text.GoTextFace) {
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
	e.textOp.GeoM.Translate(localX, localY-fontSize-5)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(e.nowPlayingBuffer, songTitle, e.titleFace, e.textOp)

	now := time.Now()
	glitchDuration := 2 * time.Second
	isGlitching := now.Sub(e.songChangedAt) < glitchDuration
	intensity := 0.0
	if isGlitching {
		intensity = 1.0 - (now.Sub(e.songChangedAt).Seconds() / glitchDuration.Seconds())
	}

	e.drawMarquee(e.nowPlayingBuffer, e.CurrentSong, face, localX, localY+fontSize*0.2, songBoxW, 0.8, isGlitching, intensity, &e.songBuffer)

	yOffset := fontSize * 1.1
	if e.CurrentArtist != "" {
		e.drawMarquee(e.nowPlayingBuffer, e.CurrentArtist, e.artistFace, localX, localY+yOffset, songBoxW, 0.5, isGlitching, intensity, &e.artistBuffer)
		yOffset += fontSize * 1.1
	}

	if e.CurrentExtra != "" {
		e.drawMarquee(e.nowPlayingBuffer, e.CurrentExtra, e.extraFace, localX, localY+yOffset, songBoxW, 0.4, isGlitching, intensity, &e.extraBuffer)
	}

	e.drawGlitchImage(screen, e.nowPlayingBuffer, songX-10, songYBase-fontSize-15, 1.0, intensity, isGlitching)
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

	// 3. Bottom Right: Legend & Trendlines
	var graphW, legendW, legendH float64
	if e.Width > 2000 {
		graphW = 600.0
		legendW, legendH = 520.0, 300.0
	} else {
		graphW = 300.0
		legendW, legendH = 260.0, 150.0
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

	imgW, _ := e.pulseImage.Bounds().Dx(), e.pulseImage.Bounds().Dy()
	halfW := float64(imgW) / 2
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
	sort.Slice(e.legendRows, func(i, j int) bool { return e.legendRows[i].val > e.legendRows[j].val })

	for i, r := range e.legendRows {
		y := firehoseY + float64(i)*(fontSize+10)
		// Draw the pulse circle (swatch) - using the map color (col)
		cr, cg, cb := float32(r.col.R)/255.0, float32(r.col.G)/255.0, float32(r.col.B)/255.0
		baseAlpha := float32(0.6)
		if r.col == ColorGossip {
			baseAlpha = 0.4
		}

		e.drawOp.GeoM.Reset()
		e.drawOp.Blend = ebiten.BlendLighter
		scale := swatchSize / float64(imgW) * 1
		e.drawOp.GeoM.Translate(-halfW, -halfW)
		e.drawOp.GeoM.Scale(scale, scale)
		e.drawOp.GeoM.Translate(firehoseX+(swatchSize/2), y+(fontSize/2))
		e.drawOp.ColorScale.Reset()
		e.drawOp.ColorScale.Scale(cr*baseAlpha, cg*baseAlpha, cb*baseAlpha, baseAlpha)
		screen.DrawImage(e.pulseImage, e.drawOp)

		// Draw the text label in the legend box
		tr, tg, tb := float32(r.uiCol.R)/255.0, float32(r.uiCol.G)/255.0, float32(r.uiCol.B)/255.0
		e.textOp.GeoM.Reset()
		e.textOp.GeoM.Translate(firehoseX+swatchSize+15, y)
		e.textOp.ColorScale.Reset()
		e.textOp.ColorScale.Scale(tr, tg, tb, 0.9)
		text.Draw(screen, r.label, e.face, e.textOp)
	}

	// Draw Trendlines Box
	e.drawTrendlines(screen, gx, gy, graphW, trendBoxW, graphH, fontSize, legendH, e.legendRows)
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

	hLen := len(e.history)
	numSteps := float64(hLen - 2)
	if numSteps <= 0 {
		numSteps = 1
	}
	step := chartW / numSteps
	paddingX := math.Ceil(step * 2)

	// Use persistent buffer for trendlines to avoid per-frame allocations.
	// We make it wider by paddingX so paths can start before the left edge
	// and we can cleanly clip out only the valid graph region without drawing glitches.
	bufW := int(chartW + paddingX)
	bufH := int(chartH + 10)
	if e.trendLinesBuffer == nil || e.trendLinesBuffer.Bounds().Dx() != bufW || e.trendLinesBuffer.Bounds().Dy() != bufH {
		e.trendLinesBuffer = ebiten.NewImage(bufW, bufH)
	}
	e.trendLinesBuffer.Clear()

	e.drawTrendLayers(chartW, chartH, globalMaxLog, rows, step, paddingX)

	// SubImage out only the portion that corresponds to the chart to chop off the left side padding where strokes begin
	sub := e.trendLinesBuffer.SubImage(image.Rect(int(paddingX), 0, bufW, bufH)).(*ebiten.Image)

	// Draw the clipped trend image back to the screen
	e.drawOp.GeoM.Reset()
	// Compensate for the SubImage crop by translating left by paddingX (which is already chopped)
	e.drawOp.GeoM.Translate(gx-paddingX, gy+titlePadding)
	e.drawOp.ColorScale.Reset()
	e.drawOp.ColorScale.Scale(1, 1, 1, 0.8) // Apply transparency globally
	e.drawOp.Blend = ebiten.BlendSourceOver
	screen.DrawImage(sub, e.drawOp)
}

func (e *Engine) calculateGlobalMaxLog() float64 {
	globalMaxLog := 1.0
	for _, s := range e.history {
		for _, val := range []int{s.New, s.Upd, s.With, s.Gossip} {
			if l := e.logVal(val); l > globalMaxLog {
				globalMaxLog = l
			}
		}
	}
	return globalMaxLog
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

func (e *Engine) drawTrendLayers(chartW, chartH, globalMaxLog float64, rows []legendRow, step, paddingX float64) {
	smoothOffset := 1.0
	if time.Since(e.lastMetricsUpdate) <= 1*time.Second {
		smoothOffset = math.Mod(time.Since(e.lastMetricsUpdate).Seconds(), 1.0)
	}

	hLen := len(e.history)

	for i := len(rows) - 1; i >= 0; i-- {
		r := rows[i]
		if !e.hasActivity(r) {
			continue
		}

		var path vector.Path
		started := false

		for j := 0; j < hLen-1; j++ {
			if !e.hasRecentActivity(r, j) {
				if started {
					e.strokeTrendPath(&path, r.col)
					path = vector.Path{}
					started = false
				}
				continue
			}

			x1 := (float64(j)-smoothOffset)*step + paddingX
			x2 := (float64(j+1)-smoothOffset)*step + paddingX
			y1 := chartH - (e.logVal(r.accessor(e.history[j]))/globalMaxLog)*chartH
			y2 := chartH - (e.logVal(r.accessor(e.history[j+1]))/globalMaxLog)*chartH

			if !started {
				path.MoveTo(float32(x1), float32(y1))
				started = true
			}
			path.LineTo(float32(x2), float32(y2))
		}

		if started {
			e.strokeTrendPath(&path, r.col)
		}
	}
}

func (e *Engine) strokeTrendPath(path *vector.Path, c color.RGBA) {
	c.A = 255 // Draw opaque into buffer to avoid blending artifacts
	e.vectorDrawPathOp.ColorScale.Reset()
	e.vectorDrawPathOp.ColorScale.ScaleWithColor(c)
	e.vectorDrawPathOp.Blend = ebiten.BlendSourceOver
	vector.StrokePath(e.trendLinesBuffer, path, &e.vectorStrokeOp, &e.vectorDrawPathOp)
}

func (e *Engine) hasActivity(r legendRow) bool {
	for _, s := range e.history {
		if r.accessor(s) > 0 {
			return true
		}
	}
	return false
}

func (e *Engine) hasRecentActivity(r legendRow, j int) bool {
	val1 := r.accessor(e.history[j])
	val2 := r.accessor(e.history[j+1])
	if val1 > 0 || val2 > 0 {
		return true
	}
	windowStart := j - 5
	if windowStart < 0 {
		windowStart = 0
	}
	windowEnd := j + 5
	if windowEnd >= len(e.history) {
		windowEnd = len(e.history) - 1
	}
	for k := windowStart; k <= windowEnd; k++ {
		if r.accessor(e.history[k]) > 0 {
			return true
		}
	}
	return false
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
			e.updateVisualImpacts(uiInterval, firstRun)

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
}

func (e *Engine) updateVisualHubs(uiInterval float64, firstRun bool) {
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

	for _, vh := range e.VisualHubs {
		vh.Active = false
		vh.TargetAlpha = 0.0
	}

	for i := 0; i < maxItems; i++ {
		if current[i].rate < 0.1 && !firstRun {
			continue
		}

		targetY := hubYBase + float64(i)*spacing
		rateStr := fmt.Sprintf("%.0f", current[i].rate)
		tw, _ := text.Measure(rateStr, e.monoFace, 0)
		if vh, ok := e.VisualHubs[current[i].cc]; ok {
			vh.Active = true
			vh.TargetY = targetY
			vh.TargetAlpha = 1.0
			vh.Rate = current[i].rate
			vh.RateStr = rateStr
			vh.RateWidth = tw
		} else {
			countryName := countries.ByName(current[i].cc).String()
			if countryName == "Unknown" {
				countryName = current[i].cc
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

			e.VisualHubs[current[i].cc] = &VisualHub{
				CC:          current[i].cc,
				CountryStr:  countryName,
				Rate:        current[i].rate,
				RateStr:     rateStr,
				RateWidth:   tw,
				DisplayY:    targetY,
				TargetY:     targetY,
				Alpha:       0,
				TargetAlpha: 1.0,
				Active:      true,
			}
		}
	}

	for cc, vh := range e.VisualHubs {
		if !vh.Active {
			delete(e.VisualHubs, cc)
		}
	}
}

func (e *Engine) updateVisualImpacts(uiInterval float64, firstRun bool) {
	type impact struct {
		prefix string
		rate   float64
	}

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

	fontSize := 18.0
	boxW, boxH := 280.0, 180.0
	if e.Width > 2000 {
		fontSize = 36.0
		boxW, boxH = 560.0, 360.0
	}
	spacing := fontSize * 1.2
	hubYBase := float64(e.Height) / 2.0
	impactYBase := hubYBase + boxH + 40.0
	if e.Width > 2000 {
		impactYBase = hubYBase + boxH + 80.0
	}

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

		asnStr := ""
		if asn != 0 {
			asnStr = fmt.Sprintf("AS%d", asn)
			if networkName != "" {
				asnStr = fmt.Sprintf("AS%d - %s", asn, networkName)
			}
		}

		rateStr := fmt.Sprintf("%.1f", allImpact[i].rate)
		tw, _ := text.Measure(rateStr, e.monoFace, 0)

		var asnLines []string
		if asnStr != "" {
			charW, _ := text.Measure("A", e.subMonoFace, 0)
			maxChars := int((boxW - 25) / charW)
			if maxChars < 10 {
				maxChars = 10
			}
			asnLines = wrapString(asnStr, maxChars, 2)
		}

		if vi, ok := e.VisualImpact[allImpact[i].prefix]; ok {
			vi.Active = true
			vi.TargetY = targetY
			vi.TargetAlpha = 1.0
			vi.Count = allImpact[i].rate
			vi.ASN = asn
			vi.NetworkName = networkName
			vi.asnStr = asnStr
			vi.asnLines = asnLines
			vi.RateStr = rateStr
			vi.RateWidth = tw
		} else {
			e.VisualImpact[allImpact[i].prefix] = &VisualImpact{
				Prefix:      allImpact[i].prefix,
				ASN:         asn,
				NetworkName: networkName,
				asnStr:      asnStr,
				asnLines:    asnLines,
				Count:       allImpact[i].rate,
				RateStr:     rateStr,
				RateWidth:   tw,
				DisplayY:    targetY,
				TargetY:     targetY,
				Alpha:       0,
				TargetAlpha: 1.0,
				Active:      true,
			}
		}
	}

	for p, vi := range e.VisualImpact {
		if !vi.Active {
			delete(e.VisualImpact, p)
		}
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
	radius := h * 0.45
	centerX := x + (w / 2) - 10
	centerY := y + (h / 2) - 10

	// 1. Background circle
	var bgPath vector.Path
	bgPath.Arc(float32(centerX), float32(centerY), float32(radius), 0, 2*math.Pi, vector.Clockwise)
	e.vectorDrawPathOp.ColorScale.Reset()
	e.vectorDrawPathOp.ColorScale.ScaleWithColor(color.RGBA{30, 30, 30, 255})
	vector.FillPath(screen, &bgPath, &e.vectorFillOp, &e.vectorDrawPathOp)

	// 2. Beacon slice
	if e.displayBeaconPercent > 0.01 {
		var beaconPath vector.Path
		startAngle := -math.Pi / 2 // Top
		endAngle := startAngle + (2 * math.Pi * e.displayBeaconPercent / 100.0)

		beaconPath.MoveTo(float32(centerX), float32(centerY))
		beaconPath.Arc(float32(centerX), float32(centerY), float32(radius), float32(startAngle), float32(endAngle), vector.Clockwise)
		beaconPath.Close()

		e.vectorDrawPathOp.ColorScale.Reset()
		e.vectorDrawPathOp.ColorScale.ScaleWithColor(color.RGBA{255, 165, 0, 255})
		vector.FillPath(screen, &beaconPath, &e.vectorFillOp, &e.vectorDrawPathOp)
	}

	// 3. Donut hole (mask)
	var holePath vector.Path
	holeRadius := radius * 0.7
	holePath.Arc(float32(centerX), float32(centerY), float32(holeRadius), 0, 2*math.Pi, vector.Clockwise)

	e.vectorDrawPathOp.ColorScale.Reset()
	e.vectorDrawPathOp.ColorScale.ScaleWithColor(color.RGBA{8, 10, 15, 255})
	vector.FillPath(screen, &holePath, &e.vectorFillOp, &e.vectorDrawPathOp)

	// 4. Percentage Text (in middle of donut)
	percentStr := fmt.Sprintf("%.0f%%", e.displayBeaconPercent)
	tw, _ := text.Measure(percentStr, e.titleFace09, 0)
	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(centerX-(tw/2), centerY-(fontSize/2))
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.9)
	text.Draw(screen, percentStr, e.titleFace09, e.textOp)

	// 5. Small Legend Underneath
	legY := centerY + radius + fontSize*0.6
	swatchSize := fontSize * 0.4

	// Calculate combined width of both legend items
	bStr, oStr := "BEACON", "ORGANIC"
	btw, _ := text.Measure(bStr, e.titleFace05, 0)
	otw, _ := text.Measure(oStr, e.titleFace05, 0)

	// Total width = swatch + gap + text + padding + swatch + gap + text
	itemGap := 20.0
	bItemW := swatchSize + 5 + btw
	oItemW := swatchSize + 5 + otw
	totalLegW := bItemW + itemGap + oItemW

	legX := x + (w / 2) - (totalLegW / 2) - 10 // Center relative to donut center (-10 is from centerX calculation)

	// Beacon Legend Item
	vector.FillRect(screen, float32(legX), float32(legY), float32(swatchSize), float32(swatchSize), color.RGBA{255, 165, 0, 255}, false)
	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(legX+swatchSize+5, legY-fontSize*0.1)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.6)
	text.Draw(screen, bStr, e.titleFace05, e.textOp)

	// Organic Legend Item
	legX += bItemW + itemGap
	vector.FillRect(screen, float32(legX), float32(legY), float32(swatchSize), float32(swatchSize), color.RGBA{30, 30, 30, 255}, false)
	e.textOp.GeoM.Reset()
	e.textOp.GeoM.Translate(legX+swatchSize+5, legY-fontSize*0.1)
	e.textOp.ColorScale.Reset()
	e.textOp.ColorScale.Scale(1, 1, 1, 0.6)
	text.Draw(screen, oStr, e.titleFace05, e.textOp)
}

func wrapString(s string, maxChars, maxLines int) []string {
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

		switch {
		case currentLine == "":
			currentLine = word
		case len(currentLine)+1+len(word) <= maxChars:
			currentLine += " " + word
		default:
			lines = append(lines, currentLine)
			currentLine = word
		}
	}

	if currentLine != "" {
		lines = append(lines, currentLine)
	}

	return lines
}
