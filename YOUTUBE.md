Experience the hidden infrastructure of the global internet.

This live stream visualizes real-time BGP (Border Gateway Protocol) update messages as they ripple across the globe. Every pulse represents a routing change, a network link flapping, or a critical withdrawal of connectivity from the global routing table.

🔴 WHAT YOU ARE SEEING:
The map shows BGP updates sourced from RIPE NCC's RIS Live service:
[https://ris-live.ripe.net/](https://ris-live.ripe.net/)
These updates are the "gossip" of the internet. It is how different networks (Autonomous Systems) tell each other how to reach IP addresses.

🎨 THE LEGEND (CLASSIFICATION ENGINE):
Updates are processed through a multi-stage classification engine and categorized into four severity tiers based on network behavior:

* Red Pulses (Critical): Outages, Route Leaks, and BGP Hijacks. These are significant routing failures or security events, such as a prefix losing all its paths, path violations suggesting a route leak, or RPKI invalid status.
* Orange Pulses (Bad Behavior): Flaps. This highlights highly volatile or inefficient routing, including rapid toggling of reachability or continuous next-hop oscillation.
* Purple Pulses (Policy & Path Hunting): Standard routing adjustments and DDoS Mitigations. This includes traffic engineering (Policy Churn, Path Length Oscillation), standard RTBH (Remotely Triggered Black Hole) announcements to mitigate DDoS attacks, or the natural "Path Hunting" process where routers actively explore alternative routes during convergence.
* Blue Pulses (Discovery & Gossip): Routine background noise. This covers standard prefix origination and redundant gossip pulses that keep routing tables current.

🌀 WATCHING THE INTERNET SELF-HEAL:
Occasionally, you will see a sudden, massive wave of Purple (Path Hunting) pulses sweep across the globe simultaneously. You are witnessing the Internet’s "immune system" in action:

* Self-Correction in Real-Time: When a major global fiber cable is cut or a high-capacity backbone router fails, thousands of networks lose their primary path.
* The Global Recalculation: Instead of staying disconnected, the global routing system automatically "hunts" for new paths. This massive burst of purple is the sound of millions of routers worldwide re-mapping the internet to bypass the failure.
* Resiliency in Action: You are watching the internet heal itself at the speed of light, routing around damage to ensure the world stays connected.

📡 ONGOING RESEARCH & VALIDATION (BEACONS):
The stream includes an analysis panel tracking the percentage of BGP updates belonging to "routing beacons". Routing beacons will announce and withdraw their routes on a strict schedule. Think of them as a scheduled pulse check for the internet, providing a way for ongoing research and validation of the Internet backbone. By watching how fast these test signals ripple across the globe, we can measure the overall health, speed, and reaction time of the global routing system.
Learn more about routing beacons here:
[https://ris.ripe.net/docs/routing-beacons/](https://ris.ripe.net/docs/routing-beacons/)

⚙️ TECHNICAL SPECS:

* Resolution: 3840x2160 @ 30fps
* Data Source: RIPE NCC RIS Live (Real-time)
* Engine: Built with Go and Ebitengine
* Map Projection: Mollweide

🔗 PROJECT LINKS:
This project is open-source! Check out the code, run your own viewer, or contribute on GitHub:
[https://github.com/sudorandom/bgp-stream](https://github.com/sudorandom/bgp-stream)

Read the full technical breakdown of how this map was built:
[https://kmcd.dev/posts/live-internet-map/](https://kmcd.dev/posts/live-internet-map/)

🎵 MUSIC:
Featuring a curated playlist of ambient and lo-fi tracks to accompany the digital heartbeat of the world. Music Sourced from https://freetouse.com/music

Technical deep dive: https://kmcd.dev/posts/live-internet-map/

#BGP #Internet #Networking #Visualization #Cybersecurity #DataViz #4K #LiveStream #Technology
