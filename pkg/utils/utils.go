// Package utils provides various utility functions and data structures for BGP stream processing.
package utils

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

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
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v", err)
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
	resp, err := http.Head(url)
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
	cacheDir := "data/cache"
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
		cacheDir := "data/cache"
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
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
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
