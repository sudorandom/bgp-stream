package bgpengine

import (
	"fmt"
	"image/color"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/biter777/countries"
	"github.com/hajimehoshi/ebiten/v2"
	"github.com/hajimehoshi/ebiten/v2/text/v2"
	"github.com/hajimehoshi/ebiten/v2/vector"
)

var (
	lastTime time.Time
)

func (e *Engine) drawStatus(screen *ebiten.Image) {
	if e.monoSource == nil {
		return
	}
	now := time.Now()
	fontSize, margin := 36.0, 100.0
	if e.Width > 2000 {
		fontSize, margin = 72.0, 200.0
	}
	h, m := now.Hour(), now.Minute()
	lh, lm := lastTime.Hour(), lastTime.Minute()
	lastTime = now
	parts := []struct {
		val       int
		format    string
		changed   bool
		intensity float64
	}{
		{h, "%02d", h != lh, 5.0}, {0, ":", false, 0}, {m, "%02d", m != lm, 2.5},
	}
	x, y := margin, float64(e.Height)-margin
	face := &text.GoTextFace{Source: e.monoSource, Size: fontSize}
	ms := now.UnixNano() / 1e6 % 1000
	for _, p := range parts {
		str := p.format
		if p.format != ":" {
			str = fmt.Sprintf(p.format, p.val)
		}
		tw, _ := text.Measure(str, face, 0)
		glitchLimit := 150.0
		if p.intensity > 1.0 {
			glitchLimit = 300.0 * (p.intensity / 2)
		}
		isGlitching := float64(ms) < glitchLimit
		jx, jy := 0.0, 0.0
		if p.changed && isGlitching {
			jx = (rand.Float64() - 0.5) * (fontSize / 4) * p.intensity
			jy = (rand.Float64() - 0.5) * (fontSize / 6) * p.intensity
		}
		if p.changed && isGlitching {
			offset := 2.0 * p.intensity
			ro := &text.DrawOptions{}
			ro.GeoM.Translate(x+jx+offset, y+jy)
			ro.ColorScale.Scale(1, 0, 0, 0.6)
			text.Draw(screen, str, face, ro)
			co := &text.DrawOptions{}
			co.GeoM.Translate(x+jx-offset, y+jy)
			co.ColorScale.Scale(0, 1, 1, 0.6)
			text.Draw(screen, str, face, co)
		}
		mo := &text.DrawOptions{}
		mo.GeoM.Translate(x+jx, y+jy)
		alpha := 0.9
		if p.changed && isGlitching {
			alpha = 0.3 + rand.Float64()*0.7
		}
		mo.ColorScale.Scale(1, 1, 1, float32(alpha))
		text.Draw(screen, str, face, mo)
		x += tw
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
	defer e.metricsMu.Unlock()
	face := &text.GoTextFace{Source: e.fontSource, Size: fontSize}

	// 1. Top Right: Stable Hub List
	topRightX := float64(e.Width) - margin - 250.0
	if e.Width > 2000 {
		topRightX = float64(e.Width) - margin - 500.0
	}
	titleOp := &text.DrawOptions{}
	titleOp.GeoM.Translate(topRightX, margin)
	titleOp.ColorScale.Scale(1, 1, 1, 0.5)
	text.Draw(screen, "TOP ACTIVITY HUBS", face, titleOp)
	for i, hub := range e.topHubs {
		countryName := countries.ByName(hub.CC).String()
		if countryName == "Unknown" {
			countryName = hub.CC
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

		str := fmt.Sprintf("%s: %d ops/s", countryName, hub.Rate)
		itemOp := &text.DrawOptions{}
		itemOp.GeoM.Translate(topRightX, margin+fontSize+10+float64(i)*fontSize*1.2)
		itemOp.ColorScale.Scale(1, 1, 1, 0.8)
		text.Draw(screen, str, face, itemOp)
	}

	// 2. Bottom Right (to the left of graphs): Global Event Rate
	graphW, graphH := 300.0, 100.0
	if e.Width > 2000 {
		graphW, graphH = 600.0, 200.0
	}
	firehoseX := float64(e.Width) - margin - graphW - 250.0
	if e.Width > 2000 {
		firehoseX = float64(e.Width) - margin - graphW - 500.0
	}
	firehoseY := float64(e.Height) - margin - graphH

	newOp := &text.DrawOptions{}
	newOp.GeoM.Translate(firehoseX, firehoseY+fontSize+10)
	newOp.ColorScale.Scale(float32(ColorNew.R)/255, float32(ColorNew.G)/255, float32(ColorNew.B)/255, 0.9)
	text.Draw(screen, fmt.Sprintf("ANNOUNCE: %d ops/s", int(e.rateNew)), face, newOp)

	updOp := &text.DrawOptions{}
	updOp.GeoM.Translate(firehoseX, firehoseY+(fontSize+10)*2)
	updOp.ColorScale.Scale(float32(ColorUpd.R)/255, float32(ColorUpd.G)/255, float32(ColorUpd.B)/255, 0.9)
	text.Draw(screen, fmt.Sprintf("PATH CHANGE: %d ops/s", int(e.rateUpd)), face, updOp)

	withOp := &text.DrawOptions{}
	withOp.GeoM.Translate(firehoseX, firehoseY+(fontSize+10)*3)
	withOp.ColorScale.Scale(float32(ColorWith.R)/255, float32(ColorWith.G)/255, float32(ColorWith.B)/255, 0.9)
	text.Draw(screen, fmt.Sprintf("WITHDRAWAL:  %d ops/s", int(e.rateWith)), face, withOp)

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
	maxVal := 10.0
	for _, s := range e.history {
		sum := float64(s.New + s.Upd + s.With)
		if sum > maxVal {
			maxVal = sum
		}
	}
	drawLayer := func(getColor func(s MetricSnapshot) int, col color.RGBA) {
		step := graphW / 60.0
		logVal := func(v int) float64 {
			if v <= 0 {
				return 0
			}
			return math.Log10(float64(v) + 1.0)
		}
		maxLog := logVal(int(maxVal))
		if maxLog < 1.0 {
			maxLog = 1.0
		}
		for i := 0; i < len(e.history)-1; i++ {
			x1, x2 := gx+float64(i)*step, gx+float64(i+1)*step
			y1 := gy + graphH - (logVal(getColor(e.history[i]))/maxLog)*graphH
			y2 := gy + graphH - (logVal(getColor(e.history[i+1]))/maxLog)*graphH
			vector.StrokeLine(screen, float32(x1), float32(y1), float32(x2), float32(y2), 2, col, false)
		}
	}
	drawLayer(func(s MetricSnapshot) int { return s.New }, ColorNew)
	drawLayer(func(s MetricSnapshot) int { return s.Upd }, ColorUpd)
	drawLayer(func(s MetricSnapshot) int { return s.With }, ColorWith)
}

func (e *Engine) StartMetricsLoop() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		e.metricsMu.Lock()
		snap := MetricSnapshot{New: int(e.windowNew), Upd: int(e.windowUpd), With: int(e.windowWith), Note: int(e.windowNote), Peer: int(e.windowPeer), Open: int(e.windowOpen)}
		e.rateNew, e.rateUpd, e.rateWith = float64(snap.New)/2.0, float64(snap.Upd)/2.0, float64(snap.With)/2.0
		e.rateNote, e.ratePeer, e.rateOpen = float64(snap.Note)/2.0, float64(snap.Peer)/2.0, float64(snap.Open)/2.0
		e.history = append(e.history, snap)
		if len(e.history) > 60 {
			e.history = e.history[1:]
		}
		for len(e.history) < 60 {
			e.history = append([]MetricSnapshot{{}}, e.history...)
		}
		e.windowNew, e.windowUpd, e.windowWith = 0, 0, 0
		e.windowNote, e.windowPeer, e.windowOpen = 0, 0, 0
		type hub struct {
			cc   string
			rate int
		}
		var current []hub
		for cc, val := range e.countryActivity {
			current = append(current, hub{cc, val / 2})
		}
		sort.Slice(current, func(i, j int) bool { return current[i].rate > current[j].rate })
		maxItems := 5
		if len(current) < maxItems {
			maxItems = len(current)
		}
		newTop := make([]struct {
			CC   string
			Rate int
		}, 0, maxItems)
		for i := 0; i < maxItems; i++ {
			if current[i].rate >= 2 {
				newTop = append(newTop, struct {
					CC   string
					Rate int
				}{current[i].cc, current[i].rate})
			}
		}
		e.topHubs = newTop
		e.countryActivity = make(map[string]int)
		e.metricsMu.Unlock()
	}
}
