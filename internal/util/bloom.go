package util

import (
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic set membership test.
// False positives are possible; false negatives are not.
// It is not safe for concurrent use — callers must synchronize if needed.
type BloomFilter struct {
	bits []uint64 // bit storage (each uint64 holds 64 bits)
	k    uint     // number of hash functions
	m    uint64   // total number of bits
	n    uint64   // number of items added
	buf  []byte   // reusable buffer for key hashing
}

// NewBloomFilter creates a Bloom filter sized for the given expected number of
// items with the target false positive rate. A smaller fpRate uses more memory
// but has fewer false positives.
func NewBloomFilter(expectedItems uint, fpRate float64) *BloomFilter {
	if expectedItems == 0 {
		expectedItems = 1000
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}
	m := optimalM(expectedItems, fpRate)
	k := optimalK(m, uint64(expectedItems))
	words := (m + 63) / 64
	return &BloomFilter{
		bits: make([]uint64, words),
		k:    k,
		m:    m,
		buf:  make([]byte, 0, 256),
	}
}

// Add inserts a key into the filter.
func (bf *BloomFilter) Add(key string) {
	hashes := bf.hashes(key)
	for i := uint(0); i < bf.k; i++ {
		bit := hashes[i%4] % bf.m
		word := bit / 64
		shift := bit % 64
		bf.bits[word] |= 1 << shift
	}
	bf.n++
}

// MightContain returns true if the key might have been added, or false if it
// definitely was not added.
func (bf *BloomFilter) MightContain(key string) bool {
	hashes := bf.hashes(key)
	for i := uint(0); i < bf.k; i++ {
		bit := hashes[i%4] % bf.m
		word := bit / 64
		shift := bit % 64
		if bf.bits[word]&(1<<shift) == 0 {
			return false
		}
	}
	return true
}

// Len returns the number of items added.
func (bf *BloomFilter) Len() uint64 { return bf.n }

// SizeBytes returns the memory used by the bit array in bytes.
func (bf *BloomFilter) SizeBytes() int { return len(bf.bits) * 8 }

// hashes returns 4 hash values for the key using FNV-1a 32 and 64-bit hashes.
// The 4 values are combined via double hashing to produce k positions.
func (bf *BloomFilter) hashes(key string) [4]uint64 {
	bf.buf = append(bf.buf[:0], key...)

	h32 := fnv.New32a()
	h32.Write(bf.buf)
	h1 := uint64(h32.Sum32())

	h64 := fnv.New64a()
	h64.Write(bf.buf)
	h2 := h64.Sum64()

	// Kirschner-Mitzenmacher double hashing: h(i) = h1 + i*h2
	h3 := h1 + h2
	h4 := h1 + h2 + h2

	return [4]uint64{h1, h2, h3, h4}
}

func optimalM(n uint, p float64) uint64 {
	// m = -n * ln(p) / (ln(2)^2)
	m := float64(n) * -math.Log(p) / (math.Ln2 * math.Ln2)
	return uint64(math.Ceil(m))
}

func optimalK(m, n uint64) uint {
	// k = (m/n) * ln(2)
	k := float64(m) / float64(n) * math.Ln2
	ki := uint(math.Ceil(k))
	if ki < 1 {
		return 1
	}
	if ki > 4 {
		return 4 // we only generate 4 independent hashes
	}
	return ki
}
