1. **Understand Goal**: Replace the "Critical Event Stream" with an "Internet State Report" stream. Instead of logging raw BGP anomalies as they occur, it should display aggregated/derived insights.
   - Example requests: "How many outages? How bad? What is the worst outage, grouped by city, country and or ASN"
   - "How many DDoS attacks are being mitigated and how? What's the general level of churn? What percentage of the internet is experiencing an outage? What is the noisiest ASN or prefix? Is it from flapping?"
   - "trigger conditionally, only emitting values that meet a threshold or are interesting enough."

2. **Analysis**:
   - The current `CriticalStream` slice holds `*CriticalEvent` structs and is populated in `recordToCriticalStream`.
   - We need to completely revamp this logic. Instead of pushing to `CriticalStream` upon every outage/leak/hijack, we will generate "Insights" periodically (e.g., every 5-10 seconds) based on the state in `statsWorkerState` / `MetricSnapshot`, OR we can generate them during the stats worker's 1-second interval if there are notable changes.
   - It's easier to create a new `InsightEvent` type (or reuse `CriticalEvent` but rename/repurpose it) that contains:
     - Timestamp
     - Insight Type/Topic (Outage, DDoS, Churn, ASN Flap, etc)
     - Message Title (e.g. "MASSIVE OUTAGE")
     - Details (e.g. "AS1234 is experiencing a severe outage affecting 10k IPs.")
     - Severity/Color
   - We will replace `CriticalStream` with `InsightStream` (or just keep `CriticalStream` slice but change how we populate it and what we render).
   - The rendering logic (`drawCriticalStream`) currently expects `CriticalEvent` structure (e.g., `CachedLeakerLabel`, `CachedVictimVal`). We need to simplify the rendering to show a title + body text instead.

3. **Steps**:
   - Rename/redefine `CriticalEvent` -> `InsightEvent` (or just change `CriticalEvent` to be more generic). Let's define a new struct for clarity, e.g., `InsightEvent`, and replace `CriticalStream` with `InsightStream []*InsightEvent`.
   - Update `drawCriticalStream` to `drawInsightStream`. Render a Title + multiple lines of detail text + an Icon/Color based on the insight type.
   - Stop calling `e.recordToCriticalStream` in `engine.go:1495` entirely. Remove `recordToCriticalStream`.
   - Add a new function `generateInsights()` in `stats_worker.go` that is called at the end of `processStatsTrigger`. This function will evaluate the current global state and decide if an insight should be pushed.
   - **Insight generation logic**:
     - Keep track of "Last reported" time for each insight type/ASN to avoid spam.
     - **Outages**: Count active outages (`bgp.ClassificationOutage`). If `CritIPs` > threshold (e.g., 50k IPs), or if there's a specific ASN with >100 prefixes down. Report worst ASN (`ActiveASNImpacts` can help here).
     - **DDoS**: Look at `countMap` for DDoS Mitigation. If >0, report: "Mitigating X prefixes. Methods: RTBH, Flowspec, etc." (need to aggregate methods if possible, or just report total).
     - **Churn**: Look at `MetricSnapshot.New/Upd/With` rates. If rate > X (e.g. > 1000/s), report "High Global Churn".
     - **Noisiest ASN**: Keep track of message count per ASN over the 60s window. If an ASN exceeds Y msgs, report "Noisiest ASN".
     - Percentage of internet down: `CritIPs / TotalIPv4 * 100`. If >0.1%, report it.
   - Ensure the new stream cleans up old insights (e.g., remove after 10-15 minutes).
   - Details of Insight logic:
     - `generateInsights(state *statsWorkerState)` called inside `processStatsTrigger`.
     - We can track `lastInsightTime` for different categories to enforce a cooldown (e.g. 30s-60s per category).
     - **Outages Insight**:
       - Iterate through `state.asnGroups` looking for `Anom == bgp.ClassificationOutage.String()`.
       - Find the ASN with the most prefixes down, or the highest total count, or max IPs impacted.
       - Calculate total number of outages (number of prefixes/ASNs in Outage).
       - Determine % of internet down: sum of IPs down / (1 << 32). Or simply use the IP count.
     - **DDoS Insight**:
       - Iterate through `state.asnGroups` looking for `Anom == bgp.ClassificationDDoSMitigation.String()`.
       - Find the number of distinct ASNs doing mitigation and total prefixes. Note the mitigation types (RTBH, Flowspec) if available.
     - **Churn Insight**:
       - Look at `e.rateUpd + e.rateWith`. If it's over e.g. 500 msgs/s.
     - **Noisiest ASN Insight**:
       - We need to track the noisiest ASN over the last minute. Currently `stats_worker` tracks `anomalyActivity` per bucket. We might need a separate bucket or just iterate `state.buckets` to find the ASN with highest message count.
       - Wait, `e.windowFlap` or `state.buckets` can be used. Or we can just sum up the raw message counts per ASN in the 60s window. Let's add an `asnActivity map[uint32]int` to `windowBucket` and sum it up.
     - Let's replace `CriticalStream []*CriticalEvent` with `InsightStream []*InsightEvent`.
     - `InsightEvent` structure:
       ```go
       type InsightEvent struct {
           Timestamp time.Time
           Category  string
           Title     string
           Details   []InsightDetail
           Color     color.RGBA
       }
       type InsightDetail struct {
           Label string
           Value string
       }
       ```
     - Fix `engine_test.go` and `critical_stream_test.go` (maybe rewrite/remove tests for the old stream).
