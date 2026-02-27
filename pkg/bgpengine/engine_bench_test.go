package bgpengine

import (
	"math/rand"
	"testing"

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
