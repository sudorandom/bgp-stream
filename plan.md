1. **Implement `bgpengine.Now()` for Simulation Time**
   - Replace all instances of `time.Now()` with `bgpengine.Now()` inside the `pkg/bgpengine` package.
   - Create `pkg/bgpengine/time.go` that exports `var Now = time.Now`. This allows testing and offline-processing tools to hook the internal time representation to a mock or simulation time.

2. **Implement `VideoCapture` for output to `ffmpeg`**
   - Create `pkg/bgpengine/video_capture.go`
   - Spawns `ffmpeg -f rawvideo -vcodec rawvideo -s WxH -pix_fmt rgba -r FPS -i - -c:v libx264 -preset ultrafast -crf 18 output.mp4`.
   - Receives frames directly by reading the Ebiten screen instance's pixels into an RGBA frame and piping that through stdout to the `ffmpeg` subprocess.

3. **Implement `MRTProcessor` for Parsing BGP Traffic Offline**
   - Create `pkg/bgpengine/mrt_processor.go`
   - Uses `github.com/osrg/gobgp/v3/pkg/packet/mrt` to parse MRT files (like RIPE dumps).
   - Reads each message from the file, decodes BGP UPDATEs.
   - Forges a `RISMessageData` payload similar to the WebSocket payload that `BGPProcessor` uses.
   - Passes the synthesized payload down `m.processor.handleRISMessage(data, ...)`.
   - Advances simulation time `bgpengine.Now()` continuously and ticks the engine's `Update()` and `Draw()` in a tight loop based on the simulation speed.

4. **Create `cmd/bgp-video-generator` binary**
   - Add a new command binary `cmd/bgp-video-generator/main.go`.
   - Take parameters: `-output`, `-fps`, `-speed`, `-width`, `-height`, and positional input file.
   - Load engine offline (no standard game loop from `ebiten.RunGame`).
   - Trigger the `MRTProcessor.Process()` workflow to decode messages and save out the video.

5. **Run tests**
   - Make sure all package tests pass.
   - Run linter/formatting on new files.
   - Fix pre-commit hooks and issues if needed.
