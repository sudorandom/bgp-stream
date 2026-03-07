import re

with open('README.md', 'r') as f:
    content = f.read()

new_section = """### Anomalies and Behaviors

The classification engine also maps events into Level 2 categorizations (anomalies) based on heuristics applied over recent activity windows. These fall into three severity tiers:

**Critical (Red)**
- **Outage:** A prefix loses all its paths, requiring multiple peers and hosts to withdraw their paths to confirm.
- **Route Leak:** The AS path violates the valley-free routing principle (e.g., hairpin turns or lateral infections).
- **BGP Hijack:** A prefix is announced with an RPKI invalid status, requiring high consensus among peers and hosts.

**Bad (Orange)**
- **Flap:** A prefix experiences rapid toggling of reachability or continuous next-hop oscillation.

**Normal / Policy (Purple & Blue)**
- **DDoS Mitigation:** A prefix is announced with a standard RTBH community (65535:666) or as a highly specific /32 (IPv4) or /128 (IPv6) route.
- **Traffic Eng.:** Elevated changes in Community, AS Path, MED, or LocalPref attributes, indicating traffic engineering or policy adjustments.
- **Path Hunting:** A sequence of announcements with strictly increasing AS path lengths followed by a withdrawal, characteristic of BGP path exploration during convergence.
- **Discovery (Blue):** Prolonged announcement activity with very few path or withdrawal changes, generally representing standard prefix origination or benign routing noise.
"""

content = re.sub(r'### Anomalies and Behaviors.*?## Real-time Processing', new_section + '\n## Real-time Processing', content, flags=re.DOTALL)

with open('README.md', 'w') as f:
    f.write(content)
