package schema

import (
	"sync"
)

// Cache provides a thread-safe in-memory cache for Avro schemas.
type Cache struct {
	mu    sync.RWMutex
	items map[uint32]string
}

// NewCache creates a new schema cache.
func NewCache() *Cache {
	return &Cache{
		items: make(map[uint32]string),
	}
}

// Get retrieves a schema from the cache.
func (c *Cache) Get(id uint32) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	schema, ok := c.items[id]
	return schema, ok
}

// Put stores a schema in the cache.
func (c *Cache) Put(id uint32, schema string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[id] = schema
}

// Delete removes a schema from the cache.
func (c *Cache) Delete(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, id)
}

// Clear removes all schemas from the cache.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[uint32]string)
}

// Size returns the number of cached schemas.
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// GetCachedSchema is a package-level helper for backward compatibility.
func GetCachedSchema(id uint32) (string, bool) {
	return defaultCache.Get(id)
}

// PutCachedSchema is a package-level helper for backward compatibility.
func PutCachedSchema(id uint32, s string) {
	defaultCache.Put(id, s)
}

var defaultCache = NewCache()
