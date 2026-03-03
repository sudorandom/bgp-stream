package utils

import (
	"testing"
)

func TestLRUCache(t *testing.T) {
	cache := NewLRUCache[string, int](2)

	cache.Add("a", 1)
	cache.Add("b", 2)

	// [b, a]

	if val, ok := cache.Get("a"); !ok || val != 1 {
		t.Errorf("Expected a=1, got %v, %v", val, ok)
	}
	// [a, b]

	cache.Add("c", 3)
	// [c, a], b evicted

	if _, ok := cache.Get("b"); ok {
		t.Errorf("Expected b to be evicted")
	}

	if val, ok := cache.Get("a"); !ok || val != 1 {
		t.Errorf("Expected a=1, got %v, %v", val, ok)
	}
	// [a, c]

	if val, ok := cache.Get("c"); !ok || val != 3 {
		t.Errorf("Expected c=3, got %v, %v", val, ok)
	}
	// [c, a]

	cache.Add("d", 4)
	// [d, c], a evicted

	if _, ok := cache.Get("a"); ok {
		t.Errorf("Expected a to be evicted")
	}

	if val, ok := cache.Get("c"); !ok || val != 3 {
		t.Errorf("Expected c=3, got %v, %v", val, ok)
	}
}
