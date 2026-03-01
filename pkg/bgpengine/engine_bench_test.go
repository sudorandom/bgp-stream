package bgpengine

import (
	"math/rand"
	"testing"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
)

// BenchmarkDrawBGPStatus measures the allocations and performance of the BGP status rendering.
// High allocations per op here usually indicate that something is being created every frame.
func BenchmarkDrawBGPStatus(b *testing.B) {
	width, height := 1920, 1080
	e := NewEngine(width, height, 1.0)
	e.InitPulseTexture()

	// Pre-fill history to ensure we are benchmarking the actual drawing logic
	for i := 0; i < 60; i++ {
		e.history[i] = MetricSnapshot{
			New:    rand.Intn(100),
			Upd:    rand.Intn(100),
			With:   rand.Intn(100),
			Gossip: rand.Intn(100),
		}
	}

	screen := ebiten.NewImage(width, height)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e.DrawBGPStatus(screen)
	}
}

// TestDrawBGPStatusAllocations enforces an upper limit on allocations during the BGP status render.
// While not yet zero, this test prevents regressions from adding more heap objects to the loop.
func TestDrawBGPStatusAllocations(t *testing.T) {
	width, height := 1920, 1080
	e := NewEngine(width, height, 1.0)
	e.InitPulseTexture()

	// Fill history
	for i := 0; i < 60; i++ {
		e.history[i] = MetricSnapshot{
			New:    rand.Intn(100),
			Upd:    rand.Intn(100),
			With:   rand.Intn(100),
			Gossip: rand.Intn(100),
		}
	}

	screen := ebiten.NewImage(width, height)

	// We've optimized this down from ~8700 to <2400 allocations per frame.
	// This guardrail ensures we don't regress back to high-allocation behavior.
	const maxAllowedAllocs = 3000
	allocs := testing.AllocsPerRun(10, func() {
		e.DrawBGPStatus(screen)
	})

	if allocs > maxAllowedAllocs {
		t.Errorf("Too many allocations in DrawBGPStatus: got %.2f, want <= %d", allocs, maxAllowedAllocs)
	}
}

// BenchmarkDrawMap measures the performance of drawing pulses on the map.
func BenchmarkDrawMap(b *testing.B) {
	width, height := 1920, 1080
	e := NewEngine(width, height, 1.0)
	e.InitPulseTexture()

	// Fill with pulses
	now := time.Now()
	for i := 0; i < 500; i++ {
		e.pulses = append(e.pulses, &Pulse{
			X:         rand.Float64() * float64(width),
			Y:         rand.Float64() * float64(height),
			StartTime: now,
			Color:     ColorNew,
			MaxRadius: 100,
		})
	}

	screen := ebiten.NewImage(width, height)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e.Draw(screen)
	}
}

// TestDrawMapAllocations ensures that drawing the map doesn't introduce excessive allocations.
func TestDrawMapAllocations(t *testing.T) {
	width, height := 1920, 1080
	e := NewEngine(width, height, 1.0)
	e.InitPulseTexture()

	// Fill with pulses
	now := time.Now()
	for i := 0; i < 500; i++ {
		e.pulses = append(e.pulses, &Pulse{
			X:         rand.Float64() * float64(width),
			Y:         rand.Float64() * float64(height),
			StartTime: now,
			Color:     ColorNew,
			MaxRadius: 100,
		})
	}

	screen := ebiten.NewImage(width, height)

	// We've optimized this down from ~9500 to <2100 allocations per frame.
	// This guardrail ensures we don't regress back to high-allocation behavior.
	const maxAllowedAllocs = 2500
	allocs := testing.AllocsPerRun(10, func() {
		e.Draw(screen)
	})

	if allocs > maxAllowedAllocs {
		t.Errorf("Too many allocations in Draw: got %.2f, want <= %d", allocs, maxAllowedAllocs)
	}
}
