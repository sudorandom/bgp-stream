package utils

import (
	"container/list"
)

// LRUCache is a simple LRU cache. It is NOT thread-safe.
type LRUCache[K comparable, V any] struct {
	capacity  int
	items     map[K]*list.Element
	evictList *list.List
}

type lruEntry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRUCache creates a new LRUCache with the given capacity.
func NewLRUCache[K comparable, V any](capacity int) *LRUCache[K, V] {
	return &LRUCache[K, V]{
		capacity:  capacity,
		items:     make(map[K]*list.Element),
		evictList: list.New(),
	}
}

// Get looks up a key's value from the cache.
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*lruEntry[K, V]).value, true
	}
	var zero V
	return zero, false
}

// Add adds a value to the cache.
func (c *LRUCache[K, V]) Add(key K, value V) {
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*lruEntry[K, V]).value = value
		return
	}

	ent := &lruEntry[K, V]{key, value}
	element := c.evictList.PushFront(ent)
	c.items[key] = element

	if c.evictList.Len() > c.capacity {
		c.removeOldest()
	}
}

// removeOldest removes the oldest item from the cache.
func (c *LRUCache[K, V]) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.evictList.Remove(ent)
		kv := ent.Value.(*lruEntry[K, V])
		delete(c.items, kv.key)
	}
}

// Len returns the number of items in the cache.
func (c *LRUCache[K, V]) Len() int {
	return c.evictList.Len()
}

// Clear purges all items from the cache.
func (c *LRUCache[K, V]) Clear() {
	c.items = make(map[K]*list.Element)
	c.evictList.Init()
}
