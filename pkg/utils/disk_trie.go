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

func OpenDiskTrie(path string) (*DiskTrie, error) {
	opts := badger.DefaultOptions(path)
	// Decrease logging verbosity
	opts.Logger = nil
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

func (t *DiskTrie) BatchInsertRaw(entries map[string][]byte) error {
	wb := t.db.NewWriteBatch()
	defer wb.Cancel()

	for k, v := range entries {
		if err := wb.Set([]byte(k), v); err != nil {
			return err
		}
	}
	return wb.Flush()
}

func (t *DiskTrie) Get(key string) ([]byte, error) {
	var val []byte
	err := t.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
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

func (t *DiskTrie) ForEach(fn func(k []byte, v []byte) error) error {
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
