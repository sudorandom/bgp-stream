// Package bgpengine provides the core logic for the BGP stream engine, including geographic services.
package bgpengine

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync"

	"github.com/sudorandom/bgp-stream/pkg/sources"
)

type GeoService struct {
	width, height     int
	scale             float64
	countryHubs       map[string][]CityHub
	prefixToCityCache map[uint32]cacheEntry
	cacheMu           sync.Mutex
	prefixData        PrefixData
	cityCoords        map[string][2]float32
	cloudTrie         *sources.CloudTrie
}

func NewGeoService(width, height int, scale float64) *GeoService {
	return &GeoService{
		width:             width,
		height:            height,
		scale:             scale,
		countryHubs:       make(map[string][]CityHub),
		prefixToCityCache: make(map[uint32]cacheEntry),
		cityCoords:        make(map[string][2]float32),
	}
}

func (g *GeoService) GetIPCoords(ip uint32) (lat, lng float64, countryCode string) {
	g.cacheMu.Lock()
	if c, ok := g.prefixToCityCache[ip]; ok {
		g.cacheMu.Unlock()
		return c.Lat, c.Lng, c.CC
	}
	g.cacheMu.Unlock()

	lat, lng, countryCode = g.resolveIP(ip)

	if lat == 0 && lng == 0 {
		return 0, 0, ""
	}

	g.updateCityCache(ip, lat, lng, countryCode)

	return lat, lng, countryCode
}

func (g *GeoService) resolveIP(ip uint32) (lat, lng float64, countryCode string) {
	var city string

	// 1. Check CloudTrie first
	if g.cloudTrie != nil {
		ipObj := make(net.IP, 4)
		binary.BigEndian.PutUint32(ipObj, ip)
		if loc, ok := g.cloudTrie.Lookup(ipObj); ok {
			parts := strings.Split(loc, "|")
			if len(parts) == 2 {
				city, countryCode = parts[0], parts[1]
				lat, lng, countryCode = g.ResolveCityToCoords(city, countryCode)
			}
		}
	}

	// 2. Fallback to generic GeoIP
	if lat == 0 && lng == 0 {
		loc := g.lookupIP(ip)
		if loc != nil {
			lat, _ = loc[0].(float64)
			lng, _ = loc[1].(float64)
			countryCode, _ = loc[2].(string)
			city, _ = loc[3].(string)

			if lat == 0 && lng == 0 && city != "" {
				lat, lng, countryCode = g.ResolveCityToCoords(city, countryCode)
			}
		}
	}

	// 3. Final Fallback: Country Hubs
	if lat == 0 && lng == 0 && countryCode != "" {
		hubs := g.countryHubs[countryCode]
		if len(hubs) > 0 {
			r := rand.Float64() * hubs[len(hubs)-1].CumulativeWeight
			for _, h := range hubs {
				if h.CumulativeWeight >= r {
					lat, lng = h.Lat, h.Lng
					break
				}
			}
		}
	}
	return lat, lng, countryCode
}

func (g *GeoService) updateCityCache(ip uint32, lat, lng float64, cc string) {
	g.cacheMu.Lock()
	defer g.cacheMu.Unlock()
	if len(g.prefixToCityCache) > 100000 {
		count := 0
		for k := range g.prefixToCityCache {
			delete(g.prefixToCityCache, k)
			count++
			if count > 20000 {
				break
			}
		}
	}
	g.prefixToCityCache[ip] = cacheEntry{Lat: lat, Lng: lng, CC: cc}
}

func (g *GeoService) ResolveCityToCoords(city, cc string) (lat, lng float64, countryCode string) {
	if c, ok := g.cityCoords[fmt.Sprintf("%s|%s", strings.ToLower(city), strings.ToUpper(cc))]; ok {
		return float64(c[0]), float64(c[1]), cc
	}
	return 0, 0, cc
}

func (g *GeoService) lookupIP(ip uint32) Location {
	r := g.prefixData.R
	low, high := 0, (len(r)/2)-1
	for low <= high {
		mid := (low + high) / 2
		startIP := r[mid*2]
		nextStartIP := uint32(0xFFFFFFFF)
		if mid+1 < len(r)/2 {
			nextStartIP = r[(mid+1)*2]
		}
		if ip >= startIP && ip < nextStartIP {
			locIdx := r[mid*2+1]
			if locIdx == 4294967295 {
				return nil
			}
			return g.prefixData.L[locIdx]
		}
		if startIP < ip {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return nil
}

func (g *GeoService) Project(lat, lng float64) (x, y float64) {
	if lat > 89.5 {
		lat = 89.5
	}
	if lat < -89.5 {
		lat = -89.5
	}

	latRad, lngRad := lat*math.Pi/180, lng*math.Pi/180
	theta := latRad
	for i := 0; i < 10; i++ {
		denom := 2 + 2*math.Cos(2*theta)
		if math.Abs(denom) < 1e-9 {
			break
		}
		delta := (2*theta + math.Sin(2*theta) - math.Pi*math.Sin(latRad)) / denom
		theta -= delta
		if math.Abs(delta) < 1e-7 {
			break
		}
	}
	r := g.scale
	x = (float64(g.width) / 2) + r*(2*math.Sqrt(2)/math.Pi)*lngRad*math.Cos(theta)
	y = (float64(g.height) / 2) - r*math.Sqrt(2)*math.Sin(theta)
	return x, y
}
