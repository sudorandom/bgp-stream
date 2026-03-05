// Package utils provides various utility functions and data structures for BGP stream processing.
package utils

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type DiskTrie struct {
	db    *badger.DB
	cache sync.Map
}

func getBadgerOptions(path string) badger.Options {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil

	// Aggressive caching options for v4
	opts.IndexCacheSize = 256 << 20 // 256MB index cache
	opts.BlockCacheSize = 512 << 20 // 512MB block cache

	return opts
}

func OpenDiskTrie(path string) (*DiskTrie, error) {
	opts := getBadgerOptions(path)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &DiskTrie{db: db}, nil
}

func OpenDiskTrieReadOnly(path string) (*DiskTrie, error) {
	opts := getBadgerOptions(path)
	opts.ReadOnly = true
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &DiskTrie{db: db}, nil
}

func (t *DiskTrie) Close() error {
	return t.db.Close()
}

func (t *DiskTrie) Insert(ipNet *net.IPNet, value []byte) error {
	ip := ipNet.IP.To4()
	if ip == nil {
		return fmt.Errorf("only IPv4 supported")
	}
	ones, _ := ipNet.Mask.Size()

	// Key: IP (4 bytes) + Mask (1 byte)
	key := make([]byte, 5)
	copy(key, ip)
	key[4] = byte(ones)

	return t.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (t *DiskTrie) BatchInsert(entries map[string][]byte) error {
	if t == nil || t.db == nil {
		return nil
	}
	wb := t.db.NewWriteBatch()
	defer wb.Cancel()

	for k, v := range entries {
		_, ipNet, err := net.ParseCIDR(k)
		if err != nil {
			continue
		}
		ip := ipNet.IP.To4()
		if ip == nil {
			continue
		}
		ones, _ := ipNet.Mask.Size()
		key := make([]byte, 5)
		copy(key, ip)
		key[4] = byte(ones)
		if err := wb.Set(key, v); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (t *DiskTrie) BatchInsertUint32(entries map[uint32][]byte) error {
	if t == nil || t.db == nil {
		return nil
	}
	wb := t.db.NewWriteBatch()
	defer wb.Cancel()

	for ip, v := range entries {
		key := make([]byte, 5)
		binary.BigEndian.PutUint32(key, ip)
		key[4] = 32
		if err := wb.Set(key, v); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (t *DiskTrie) BatchInsertIPNets(entries []IPNetEntry) error {
	if t == nil || t.db == nil {
		return nil
	}
	wb := t.db.NewWriteBatch()
	defer wb.Cancel()

	for _, e := range entries {
		ip := e.Net.IP.To4()
		if ip == nil {
			continue
		}
		ones, _ := e.Net.Mask.Size()
		key := make([]byte, 5)
		copy(key, ip)
		key[4] = byte(ones)
		if err := wb.Set(key, e.Value); err != nil {
			return err
		}
	}
	return wb.Flush()
}

type IPNetEntry struct {
	Net   *net.IPNet
	Value []byte
}

func (t *DiskTrie) BatchInsertRaw(entries map[string][]byte) error {
	if t == nil || t.db == nil {
		return nil
	}
	wb := t.db.NewWriteBatch()
	defer wb.Cancel()

	for k, v := range entries {
		_, ipNet, err := net.ParseCIDR(k)
		if err == nil {
			ip := ipNet.IP.To4()
			if ip != nil {
				ones, _ := ipNet.Mask.Size()
				key := make([]byte, 5)
				copy(key, ip)
				key[4] = byte(ones)
				if err := wb.Set(key, v); err != nil {
					return err
				}
				continue
			}
		}

		if err := wb.Set([]byte(k), v); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (t *DiskTrie) Get(prefix string) ([]byte, error) {
	if t == nil || t.db == nil {
		return nil, nil
	}
	_, ipNet, err := net.ParseCIDR(prefix)
	if err != nil {
		// Fallback to raw string key if it's not a CIDR
		var val []byte
		err := t.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(prefix))
			if err != nil {
				return err
			}
			val, err = item.ValueCopy(nil)
			return err
		})
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return val, err
	}

	ip := ipNet.IP.To4()
	if ip == nil {
		return nil, fmt.Errorf("only IPv4 supported")
	}
	ones, _ := ipNet.Mask.Size()
	key := make([]byte, 5)
	copy(key, ip)
	key[4] = byte(ones)

	var val []byte
	err = t.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (t *DiskTrie) Put(prefix string, val []byte) error {
	if t == nil || t.db == nil {
		return nil
	}
	_, ipNet, err := net.ParseCIDR(prefix)
	if err != nil {
		// Fallback to raw string key if it's not a CIDR
		return t.db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(prefix), val)
		})
	}
	return t.Insert(ipNet, val)
}

type lookupResult struct {
	val     []byte
	maskLen int
}

// Lookup returns the value and mask length associated with the longest prefix matching the IP.
func (t *DiskTrie) Lookup(ip net.IP) (val []byte, maskLen int, err error) {
	target := ip.To4()
	if target == nil {
		return nil, 0, fmt.Errorf("invalid IPv4")
	}

	targetInt := binary.BigEndian.Uint32(target)
	if v, ok := t.cache.Load(targetInt); ok {
		if v == nil {
			return nil, 0, nil
		}
		res := v.(lookupResult)
		return res.val, res.maskLen, nil
	}

	var foundVal []byte
	var foundMask int
	err = t.db.View(func(txn *badger.Txn) error {
		// Key buffer to avoid allocations in the loop
		key := make([]byte, 5)
		for m := 32; m >= 0; m-- {
			var mask uint32
			if m > 0 {
				mask = uint32(0xFFFFFFFF) << (32 - m)
			} else {
				mask = 0
			}

			prefixIP := targetInt & mask
			binary.BigEndian.PutUint32(key, prefixIP)
			key[4] = byte(m)

			item, getErr := txn.Get(key)
			if getErr == nil {
				foundVal, getErr = item.ValueCopy(nil)
				foundMask = m
				return getErr
			}
		}
		return nil
	})

	if err == nil {
		if foundVal == nil {
			t.cache.Store(targetInt, nil)
		} else {
			t.cache.Store(targetInt, lookupResult{val: foundVal, maskLen: foundMask})
		}
	}
	return foundVal, foundMask, err
}

// LookupAll returns all values associated with prefixes that cover the IP.
func (t *DiskTrie) LookupAll(ip net.IP) (vals [][]byte, err error) {
	target := ip.To4()
	if target == nil {
		return nil, fmt.Errorf("invalid IPv4")
	}

	targetInt := binary.BigEndian.Uint32(target)
	err = t.db.View(func(txn *badger.Txn) error {
		key := make([]byte, 5)
		for m := 32; m >= 0; m-- {
			var mask uint32
			if m > 0 {
				mask = uint32(0xFFFFFFFF) << (32 - m)
			} else {
				mask = 0
			}

			prefixIP := targetInt & mask
			binary.BigEndian.PutUint32(key, prefixIP)
			key[4] = byte(m)

			item, getErr := txn.Get(key)
			if getErr == nil {
				val, copyErr := item.ValueCopy(nil)
				if copyErr == nil {
					vals = append(vals, val)
				}
			}
		}
		return nil
	})

	return vals, err
}

func (t *DiskTrie) LookupUint32(ip uint32) (val []byte, maskLen int, err error) {
	if v, ok := t.cache.Load(ip); ok {
		if v == nil {
			return nil, 0, nil
		}
		res := v.(lookupResult)
		return res.val, res.maskLen, nil
	}

	var foundVal []byte
	var foundMask int
	err = t.db.View(func(txn *badger.Txn) error {
		key := make([]byte, 5)
		for m := 32; m >= 0; m-- {
			var mask uint32
			if m > 0 {
				mask = uint32(0xFFFFFFFF) << (32 - m)
			} else {
				mask = 0
			}

			prefixIP := ip & mask
			binary.BigEndian.PutUint32(key, prefixIP)
			key[4] = byte(m)

			item, getErr := txn.Get(key)
			if getErr == nil {
				foundVal, getErr = item.ValueCopy(nil)
				foundMask = m
				return getErr
			}
		}
		return nil
	})

	if err == nil {
		if foundVal == nil {
			t.cache.Store(ip, nil)
		} else {
			t.cache.Store(ip, lookupResult{val: foundVal, maskLen: foundMask})
		}
	}
	return foundVal, foundMask, err
}

func (t *DiskTrie) DeleteRaw(key []byte) error {
	return t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (t *DiskTrie) ForEach(fn func(k []byte, v []byte) error) error {
	if t == nil || t.db == nil {
		return nil
	}
	return t.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				return fn(k, v)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (t *DiskTrie) IsEmpty() bool {
	if t == nil || t.db == nil {
		return true
	}
	empty := true
	err := t.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Rewind()
		if it.Valid() {
			empty = false
		}
		return nil
	})
	if err != nil {
		return true
	}
	return empty
}

func (t *DiskTrie) Clear() error {
	if t == nil || t.db == nil {
		return nil
	}
	return t.db.DropAll()
}
