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
func (t *DiskTrie) Lookup(ip net.IP) ([]byte, int, error) {
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

	var val []byte
	var foundMask int
	err := t.db.View(func(txn *badger.Txn) error {
		// Key buffer to avoid allocations in the loop
		key := make([]byte, 5)
		for maskLen := 32; maskLen >= 0; maskLen-- {
			var mask uint32
			if maskLen > 0 {
				mask = uint32(0xFFFFFFFF) << (32 - maskLen)
			} else {
				mask = 0
			}

			prefixIP := targetInt & mask
			binary.BigEndian.PutUint32(key, prefixIP)
			key[4] = byte(maskLen)

			item, err := txn.Get(key)
			if err == nil {
				val, err = item.ValueCopy(nil)
				foundMask = maskLen
				return err
			}
		}
		return nil
	})

	if err == nil {
		if val == nil {
			t.cache.Store(targetInt, nil)
		} else {
			t.cache.Store(targetInt, lookupResult{val: val, maskLen: foundMask})
		}
	}
	return val, foundMask, err
}
