package durex

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

// IDGenerator generates unique command instance IDs.
type IDGenerator interface {
	Generate() string
}

// DefaultIDGenerator creates IDs using timestamp + random bytes.
// Format: "cmd_<timestamp_hex>_<random_hex>"
// Example: "cmd_18d5f3a2b4c_8a7b6c5d4e3f2a1b"
type DefaultIDGenerator struct {
	counter uint64
}

// Generate creates a new unique ID.
func (g *DefaultIDGenerator) Generate() string {
	ts := time.Now().UnixMilli()
	count := atomic.AddUint64(&g.counter, 1)

	random := make([]byte, 8)
	_, _ = rand.Read(random)

	return fmt.Sprintf("cmd_%x_%x_%s",
		ts,
		count,
		hex.EncodeToString(random),
	)
}

// ULIDGenerator generates ULID-style IDs.
// Lexicographically sortable and URL-safe.
type ULIDGenerator struct{}

// Generate creates a new ULID.
func (g *ULIDGenerator) Generate() string {
	ts := time.Now().UnixMilli()
	random := make([]byte, 10)
	_, _ = rand.Read(random)

	// Encode timestamp (6 bytes) + random (10 bytes) in base32
	id := make([]byte, 26)
	encodeBase32(id[:10], uint64ToBytes(uint64(ts), 6))
	encodeBase32(id[10:], random)

	return string(id)
}

// SimpleIDGenerator generates simple incremental IDs.
// Useful for testing but not recommended for production.
type SimpleIDGenerator struct {
	prefix  string
	counter uint64
}

// NewSimpleIDGenerator creates a new SimpleIDGenerator with the given prefix.
func NewSimpleIDGenerator(prefix string) *SimpleIDGenerator {
	return &SimpleIDGenerator{prefix: prefix}
}

// Generate creates a new incremental ID.
func (g *SimpleIDGenerator) Generate() string {
	count := atomic.AddUint64(&g.counter, 1)
	return fmt.Sprintf("%s_%d", g.prefix, count)
}

// base32 alphabet (Crockford's)
const base32Alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func encodeBase32(dst []byte, src []byte) {
	for i := range dst {
		if i < len(src) {
			dst[i] = base32Alphabet[src[i]%32]
		} else {
			dst[i] = '0'
		}
	}
}

func uint64ToBytes(v uint64, size int) []byte {
	b := make([]byte, size)
	for i := size - 1; i >= 0; i-- {
		b[i] = byte(v & 0xff)
		v >>= 8
	}
	return b
}

// defaultIDGenerator is the package-level ID generator.
var defaultIDGenerator IDGenerator = &DefaultIDGenerator{}

// SetIDGenerator sets the package-level ID generator.
func SetIDGenerator(gen IDGenerator) {
	if gen != nil {
		defaultIDGenerator = gen
	}
}

// GenerateID creates a new ID using the package-level generator.
func GenerateID() string {
	return defaultIDGenerator.Generate()
}
