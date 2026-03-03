package bgpengine

import (
	"compress/bzip2"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hajimehoshi/ebiten/v2"
	"github.com/osrg/gobgp/v3/pkg/packet/bgp"
	"github.com/osrg/gobgp/v3/pkg/packet/mrt"
)

type MRTProcessor struct {
	processor *BGPProcessor
	filename  string
	speed     float64
	fps       int
}

func NewMRTProcessor(processor *BGPProcessor, filename string, speed float64, fps int) *MRTProcessor {
	return &MRTProcessor{
		processor: processor,
		filename:  filename,
		speed:     speed,
		fps:       fps,
	}
}

func (m *MRTProcessor) Process(ctx context.Context, engine *Engine, vc *VideoCapture) error {
	file, err := os.Open(m.filename)
	if err != nil {
		return fmt.Errorf("failed to open MRT file: %v", err)
	}
	defer file.Close()

	var reader io.Reader = file
	if strings.HasSuffix(m.filename, ".gz") {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gz.Close()
		reader = gz
	} else if strings.HasSuffix(m.filename, ".bz2") {
		reader = bzip2.NewReader(file)
	}

	var firstTimestamp time.Time
	var currentSimTime time.Time

	frameDuration := time.Second / time.Duration(m.fps)
	var nextFrameTime time.Time

	pendingWithdrawals := make(map[uint32]struct {
		Time   time.Time
		Prefix string
	})

	buf := make([]byte, 4096)

	for {
		// Read MRT header
		hdrBuf := make([]byte, mrt.MRT_COMMON_HEADER_LEN)
		_, err := io.ReadFull(reader, hdrBuf)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("failed to read MRT header: %v", err)
		}

		hdr := &mrt.MRTHeader{}
		err = hdr.DecodeFromBytes(hdrBuf)
		if err != nil {
			return fmt.Errorf("failed to decode MRT header: %v", err)
		}

		// Read MRT body
		if len(buf) < int(hdr.Len) {
			buf = make([]byte, hdr.Len)
		}
		bodyBuf := buf[:hdr.Len]
		_, err = io.ReadFull(reader, bodyBuf)
		if err != nil {
			return fmt.Errorf("failed to read MRT body: %v", err)
		}

		msg, err := mrt.ParseMRTBody(hdr, bodyBuf)
		if err != nil {
			// Skip unknown/unsupported messages
			continue
		}

		msgTime := time.Unix(int64(hdr.Timestamp), 0)

		if firstTimestamp.IsZero() {
			firstTimestamp = msgTime
			currentSimTime = msgTime
			nextFrameTime = msgTime.Add(time.Duration(float64(frameDuration) * m.speed))
			Now = func() time.Time { return currentSimTime }
		}

		if msgTime.After(currentSimTime) {
			for currentSimTime.Before(msgTime) {
				if currentSimTime.After(nextFrameTime) || currentSimTime.Equal(nextFrameTime) {
					Now = func() time.Time { return nextFrameTime }

					m.processor.mu.Lock()
					for ip, entry := range pendingWithdrawals {
						if Now().After(entry.Time) {
							if lat, lng, cc, _ := m.processor.geo(ip); cc != "" {
								m.processor.onEvent(lat, lng, cc, EventWithdrawal, Level2None, entry.Prefix, 0)
								m.processor.recentlySeen[ip] = struct {
									Time time.Time
									Type EventType
								}{Time: Now(), Type: EventWithdrawal}
							}
							delete(pendingWithdrawals, ip)
						}
					}
					m.processor.mu.Unlock()

					engine.Update()
					screen := ebiten.NewImage(engine.Width, engine.Height)
					engine.Draw(screen)

					bounds := screen.Bounds()
					rgba := image.NewRGBA(bounds)
					screen.ReadPixels(rgba.Pix)
					vc.WriteFrame(rgba)

					nextFrameTime = nextFrameTime.Add(time.Duration(float64(frameDuration) * m.speed))
				}

				step := nextFrameTime.Sub(currentSimTime)
				if msgTime.Sub(currentSimTime) < step {
					currentSimTime = msgTime
				} else {
					currentSimTime = nextFrameTime
				}
			}
		}

		Now = func() time.Time { return msgTime }

		body := msg.Body
		switch bodyType := body.(type) {
		case *mrt.BGP4MPMessage:
			bgpMsg := bodyType.BGPMessage
			if bgpMsg.Header.Type == bgp.BGP_MSG_UPDATE {
				update := bgpMsg.Body.(*bgp.BGPUpdate)

				data := &RISMessageData{}

				for _, w := range update.WithdrawnRoutes {
					data.Withdrawals = append(data.Withdrawals, w.String())
				}

				ann := struct {
					NextHop  string   `json:"next_hop"`
					Prefixes []string `json:"prefixes"`
				}{}

				for _, nlri := range update.NLRI {
					ann.Prefixes = append(ann.Prefixes, nlri.String())
				}

				for _, attr := range update.PathAttributes {
					switch a := attr.(type) {
					case *bgp.PathAttributeAsPath:
						for _, param := range a.Value {
							for _, asn := range param.GetAS() {
								data.Path = append(data.Path, json.RawMessage(strconv.FormatUint(uint64(asn), 10)))
							}
						}
					case *bgp.PathAttributeNextHop:
						ann.NextHop = a.Value.String()
					case *bgp.PathAttributeMultiExitDisc:
						data.Med = int32(a.Value)
					case *bgp.PathAttributeLocalPref:
						data.LocalPref = int32(a.Value)
					}
				}

				if len(ann.Prefixes) > 0 {
					data.Announcements = append(data.Announcements, ann)
				}

				events := m.processor.handleRISMessage(data, pendingWithdrawals)
				for _, e := range events {
					if lat, lng, cc, _ := m.processor.geo(e.ip); cc != "" {
						m.processor.onEvent(lat, lng, cc, e.eventType, e.level2Type, e.prefix, e.asn)
					}
				}
			}
		}
	}

	return nil
}
