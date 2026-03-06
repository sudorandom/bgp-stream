// Package utils provides various utility functions and data structures for BGP stream processing.
package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/bits"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// IPToUint32 converts a net.IP to uint32.
func IPToUint32(ip net.IP) uint32 {
	ip = ip.To4()
	if ip == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ip)
}

// GetPrefixSize returns the number of IPs in a CIDR prefix (e.g., /24 returns 256).
func GetPrefixSize(prefix string) uint64 {
	parts := strings.Split(prefix, "/")
	if len(parts) != 2 {
		return 0
	}
	var mask int
	if _, err := fmt.Sscanf(parts[1], "%d", &mask); err != nil {
		return 0
	}
	if mask < 0 || mask > 32 {
		return 0
	}
	return 1 << (32 - uint32(mask))
}

// RangeToCIDRs converts an IPv4 range [start, end] into a slice of *net.IPNet.
func RangeToCIDRs(start, end uint32) []*net.IPNet {
	var cidrs []*net.IPNet
	for start <= end {
		maxLen := 32 - bits.TrailingZeros32(start)
		if start == 0 {
			maxLen = 0
		}
		curLen := 32 - bits.Len32(end-start+1) + 1
		if maxLen < curLen {
			maxLen = curLen
		}

		ip := make(net.IP, 4)
		binary.BigEndian.PutUint32(ip, start)
		cidrs = append(cidrs, &net.IPNet{
			IP:   ip,
			Mask: net.CIDRMask(maxLen, 32),
		})

		// Move start to next block
		move := uint32(1) << (32 - uint32(maxLen))
		if move == 0 { // /0 block (entire space)
			break
		}

		newStart := start + move
		if newStart < start || newStart > end { // overflow or past end
			break
		}
		start = newStart
	}
	return cidrs
}

// HashUint32 returns a simple hash of a uint32, useful for deterministic mapping.
func HashUint32(x uint32) uint32 {
	x = ((x >> 16) ^ x) * 0x45d9f3b
	x = ((x >> 16) ^ x) * 0x45d9f3b
	x = (x >> 16) ^ x
	return x
}

// FormatNumber formats a large number with commas (e.g. 1,234,567).
func FormatNumber(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var res []string
	for len(s) > 3 {
		res = append([]string{s[len(s)-3:]}, res...)
		s = s[:len(s)-3]
	}
	if s != "" {
		res = append([]string{s}, res...)
	}
	return strings.Join(res, ",")
}

var ErrNotFound = errors.New("file not found on server")

type progressWriter struct {
	io.Writer
	total uint64
	last  uint64
	label string
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.Writer.Write(p)
	pw.total += uint64(n)
	if pw.total-pw.last > 5*1024*1024 { // Log every 5MB
		log.Printf("%s: Downloaded %d MB", pw.label, pw.total/1024/1024)
		pw.last = pw.total
	}
	return n, err
}

// DownloadFile downloads a file from a URL to a local path safely.
func DownloadFile(url, path string) error {
	client := &http.Client{}
	maxRetries := 2
	var resp *http.Response
	var err error

	for i := 0; i < maxRetries; i++ {
		req, err := http.NewRequest("GET", url, http.NoBody)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "bgp-stream/1.0")

		resp, err = client.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			log.Printf("Rate limited (429) for %s. Retrying in 5s...", url)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			if err := resp.Body.Close(); err != nil {
				log.Printf("Error closing response body: %v", err)
			}
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	// Create a temp file in the same directory to ensure atomic move
	tmpFile, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	defer func() {
		if err := os.Remove(tmpName); err != nil && !os.IsNotExist(err) {
			log.Printf("Error removing temp file %s: %v", tmpName, err)
		}
	}() // Clean up if we fail

	pw := &progressWriter{Writer: tmpFile, label: filepath.Base(path)}
	if _, err := io.Copy(pw, resp.Body); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	// Atomic rename to final path
	return os.Rename(tmpName, path)
}

// Exists checks if a URL exists using a HEAD request.
func Exists(url string) bool {
	client := &http.Client{}
	req, err := http.NewRequest("HEAD", url, http.NoBody)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "bgp-stream/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}()
	return resp.StatusCode == http.StatusOK
}

// GetCacheFileName returns the expected local filename for a given URL and logPrefix.
func GetCacheFileName(url, logPrefix string) string {
	urlParts := strings.Split(url, "/")
	fileName := urlParts[len(urlParts)-1]

	// Include sanitized logPrefix in the filename to prevent collisions between years/versions
	sanitizedPrefix := strings.Trim(logPrefix, "[]")
	sanitizedPrefix = strings.ReplaceAll(sanitizedPrefix, " ", "_")
	if sanitizedPrefix != "" {
		fileName = sanitizedPrefix + "_" + fileName
	}
	return fileName
}

// FindCachedURL takes a list of candidate URLs and returns the first one that exists in the local cache.
func FindCachedURL(urls []string, logPrefix string) (string, bool) {
	cacheDir := "./data/cache"
	for _, u := range urls {
		fname := GetCacheFileName(u, logPrefix)
		if _, err := os.Stat(filepath.Join(cacheDir, fname)); err == nil {
			return u, true
		}
	}
	return "", false
}

// GetCachedReader returns a reader for the given URL, using a local cache if enabled.
func GetCachedReader(url string, useCache bool, logPrefix string) (io.ReadCloser, error) {
	if useCache {
		cacheDir := "./data/cache"
		if err := os.MkdirAll(cacheDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create cache dir: %w", err)
		}
		fileName := GetCacheFileName(url, logPrefix)
		localPath := filepath.Join(cacheDir, fileName)

		if _, err := os.Stat(localPath); os.IsNotExist(err) {
			log.Printf("%s Downloading %s", logPrefix, url)
			if err := DownloadFile(url, localPath); err != nil {
				return nil, err // Return the error directly so caller can see ErrNotFound
			}
		} else {
			log.Printf("%s Using cached file: %s", logPrefix, localPath)
		}
		f, err := os.Open(localPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open cache: %w", err)
		}
		return f, nil
	}

	log.Printf("%s Streaming from %s", logPrefix, url)
	client := &http.Client{}
	maxRetries := 2
	var resp *http.Response

	for i := 0; i < maxRetries; i++ {
		var err error
		req, err := http.NewRequest("GET", url, http.NoBody)

		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "bgp-stream/1.0")

		resp, err = client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			_ = resp.Body.Close()
			log.Printf("%s Rate limited (429) for %s. Retrying in 5s...", logPrefix, url)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	if resp.StatusCode != http.StatusOK {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v", err)
		}
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("bad status: %s", resp.Status)
	}
	return resp.Body, nil
}
