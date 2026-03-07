import re

with open("pkg/bgpengine/classifier.go", "r") as f:
    text = f.read()

# remove `func (c *Classifier) isDDoSProvider(asn uint32) bool { ... }` block
text = re.sub(
    r'func \(c \*Classifier\) isDDoSProvider\(asn uint32\) bool \{[\s\S]*?return scrubbers\[asn\]\n\}',
    r'',
    text
)

with open("pkg/bgpengine/classifier.go", "w") as f:
    f.write(text)
