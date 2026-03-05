package bgpengine

import (
	"math"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/text/v2"
)

type tourRegion struct {
	name string
	lat  float64
	lng  float64
	zoom float64
}

var regions = []tourRegion{
	{"NORTH AMERICA", 30.0, -100.0, 2.5},
	{"SOUTH AMERICA", -25.0, -60.0, 2.0},
	{"EUROPE", 48.0, 15.0, 3.0},
	{"APAC", 5.0, 110.0, 1.75},
}

func (e *Engine) UpdateTour() {
	now := e.Now()
	regionStayDuration := e.tourRegionStayDuration
	tourDuration := time.Duration(len(regions)) * regionStayDuration

	// Calculate which "tour time" we should use
	// We only use manual start time now, disabling the automatic cycle
	elapsedSinceManual := now.Sub(e.tourManualStartTime)

	var elapsed time.Duration
	switch {
	case !e.tourManualStartTime.IsZero() && elapsedSinceManual < tourDuration+10*time.Second:
		elapsed = elapsedSinceManual + e.tourOffset
	case e.tourRegionIndex != -1:
		// No manual tour active
		e.tourRegionIndex = -1
		e.lastTourStateChange = e.Now()
		e.targetCX = float64(e.Width) / 2
		e.targetCY = float64(e.Height) / 2
		e.targetZoom = 1.0
		goto interpolate
	default:
		goto interpolate
	}

	if elapsed < tourDuration {
		// We are in the tour
		regionIdx := int(elapsed.Seconds() / regionStayDuration.Seconds())
		if regionIdx < len(regions) {
			if e.tourRegionIndex != regionIdx {
				// New region transition
				e.tourRegionIndex = regionIdx
				e.lastTourStateChange = e.Now()
				region := regions[regionIdx]
				e.targetCX, e.targetCY = e.geo.Project(region.lat, region.lng)
				e.targetZoom = region.zoom
			}
		}
	} else if e.tourRegionIndex != -1 { // Zoom out to full map
		e.tourRegionIndex = -1
		e.lastTourStateChange = e.Now()
		e.targetCX = float64(e.Width) / 2
		e.targetCY = float64(e.Height) / 2
		e.targetZoom = 1.0
	}

interpolate:
	// Smoothly interpolate current towards target
	// Use slightly slower interpolation for a "cinematic" feel
	lerp := 0.04
	if e.targetZoom == 1.0 && math.Abs(e.currentZoom-1.0) < 0.005 {
		// Snap when close to full map to avoid floating point jitter
		e.currentZoom = 1.0
		e.currentCX = float64(e.Width) / 2
		e.currentCY = float64(e.Height) / 2
	} else {
		e.currentZoom += (e.targetZoom - e.currentZoom) * lerp
		e.currentCX += (e.targetCX - e.currentCX) * lerp
		e.currentCY += (e.targetCY - e.currentCY) * lerp
	}
}

func (e *Engine) ApplyTourTransform(op *ebiten.DrawImageOptions) {
	op.GeoM.Reset()
	// 1. Move the map's focal point (currentCX, currentCY) to the origin (0,0)
	op.GeoM.Translate(-e.currentCX, -e.currentCY)
	// 2. Scale the map around the origin
	op.GeoM.Scale(e.currentZoom, e.currentZoom)
	// 3. Move the origin back to the screen's center
	op.GeoM.Translate(float64(e.Width)/2, float64(e.Height)/2)
}

func (e *Engine) DrawPIP(screen *ebiten.Image) {
	// Only show PIP if we are zoomed in enough
	if e.currentZoom <= 1.05 {
		return
	}

	pipScale := 0.18
	pipW, pipH := float64(e.Width)*pipScale, float64(e.Height)*pipScale
	margin := 40.0
	if e.Width > 2000 {
		margin = 80.0
	}

	x, y := margin, margin

	// Create or resize pipImage buffer if needed
	if e.pipImage == nil || e.pipImage.Bounds().Dx() != int(pipW+20) || e.pipImage.Bounds().Dy() != int(pipH+40) {
		e.pipImage = ebiten.NewImage(int(pipW+20), int(pipH+40))
	}
	e.pipImage.Clear()

	// Draw everything to the local buffer first
	localX, localY := 5.0, 5.0

	// Draw PIP background and border
	borderPadding := 2.0
	if e.whitePixel != nil {
		op := &ebiten.DrawImageOptions{}
		// Draw border
		op.GeoM.Scale(pipW+borderPadding*2, pipH+borderPadding*2)
		op.GeoM.Translate(localX-borderPadding, localY-borderPadding)
		op.ColorScale.Scale(0.2, 0.25, 0.3, 0.8) // Dark bluish border
		e.pipImage.DrawImage(e.whitePixel, op)

		// Draw black background
		op.GeoM.Reset()
		op.GeoM.Scale(pipW, pipH)
		op.GeoM.Translate(localX, localY)
		op.ColorScale.Reset()
		op.ColorScale.Scale(0, 0, 0, 1.0)
		e.pipImage.DrawImage(e.whitePixel, op)
	}

	// Draw PIP content (the full map)
	op := &ebiten.DrawImageOptions{}
	op.GeoM.Scale(pipScale, pipScale)
	op.GeoM.Translate(localX, localY)
	op.ColorScale.Scale(1, 1, 1, 0.9) // Slightly dim the PIP
	e.pipImage.DrawImage(e.mapImage, op)

	// Draw a small red indicator of where the current viewport is
	vw, vh := float64(e.Width)/e.currentZoom, float64(e.Height)/e.currentZoom
	vx, vy := e.currentCX-vw/2, e.currentCY-vh/2

	// Scale these to PIP space
	pix, piy := vx*pipScale+localX, vy*pipScale+localY
	piw, pih := vw*pipScale, vh*pipScale

	if e.whitePixel != nil {
		op := &ebiten.DrawImageOptions{}
		// Red border for the viewport indicator
		op.GeoM.Scale(piw, 2)
		op.GeoM.Translate(pix, piy)
		op.ColorScale.Scale(1, 0, 0, 0.9) // Red
		e.pipImage.DrawImage(e.whitePixel, op)

		op.GeoM.Reset()
		op.GeoM.Scale(piw, 2)
		op.GeoM.Translate(pix, piy+pih-2)
		op.ColorScale.Reset()
		op.ColorScale.Scale(1, 0, 0, 0.9) // Red
		e.pipImage.DrawImage(e.whitePixel, op)

		op.GeoM.Reset()
		op.GeoM.Scale(2, pih)
		op.GeoM.Translate(pix, piy)
		op.ColorScale.Reset()
		op.ColorScale.Scale(1, 0, 0, 0.9) // Red
		e.pipImage.DrawImage(e.whitePixel, op)

		op.GeoM.Reset()
		op.GeoM.Scale(2, pih)
		op.GeoM.Translate(pix+piw-2, piy)
		op.ColorScale.Reset()
		op.ColorScale.Scale(1, 0, 0, 0.9) // Red
		e.pipImage.DrawImage(e.whitePixel, op)
	}

	// Add region label below PIP
	if e.tourRegionIndex >= 0 && e.tourRegionIndex < len(regions) {
		regionName := regions[e.tourRegionIndex].name
		textOp := &text.DrawOptions{}
		textOp.GeoM.Translate(localX, localY+pipH+5)
		textOp.ColorScale.Scale(1, 1, 1, 0.7)
		text.Draw(e.pipImage, regionName, e.titleMonoFace, textOp)
	}

	// Now draw the pipImage to the screen stably
	opScreen := &ebiten.DrawImageOptions{}
	opScreen.GeoM.Translate(x, y)
	screen.DrawImage(e.pipImage, opScreen)
}
