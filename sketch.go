package sketchy

import "math"

var (
	DefaultEpsilon = 0.999
	DefaultDelta   = 0.99
)

// A Sketch counts occurrences of keys and returns approximate total counts.
type CountSketch interface {
	// Count adds delta to the count of occurrences of the given key.
	// Returns the updated estimated count.
	Count(key []byte, delta int) uint64

	// Query returns the estimated count of the given key.
	Query(key []byte) uint64
}

// fnvSketch provides a count-min sketch (http://en.wikipedia.org/wiki/Count-min_sketch)
// using the FNV-1 hash.
type fnvSketch struct {
	Epsilon float64
	Delta   float64
	Width   uint
	Depth   uint
	Matrix  []uint64
}

// NewSketch returns a new, empty count-min sketch with the given parameters.
// The values of epsilon and delta determine the desired accuracy of the
// sketch. Queries for the count of observations by a particular key by this
// sketch will be within a factor of epsilon of the true count, with
// probability delta.
//
// The closer these parameters are to 1, the greater the storage and
// computation cost. The value of delta determines how many hashes we must
// compute for each key (and how many counters we must inspect to answer a
// count query). The bucket uses ceil(log(1 / (1-delta))) hashes. The value
// of epsilon determines the size of the domain we map these hashes to. The
// size of the domain is e / (1-epsilon). So, for epsilon=0.999, delta=0.99,
// we would store 2719 counters for each of five hash values.
func NewSketch(epsilon, delta float64) CountSketch {
	bucket := &fnvSketch{
		Epsilon: epsilon,
		Delta:   delta,
	}
	if bucket.Epsilon == 0 {
		bucket.Epsilon = DefaultEpsilon
	}
	if bucket.Delta == 0 {
		bucket.Delta = DefaultDelta
	}
	if bucket.Matrix == nil {
		bucket.Width = uint(math.Ceil(math.E / (1 - bucket.Epsilon)))
		bucket.Depth = uint(math.Ceil(math.Log(1 / (1 - bucket.Delta))))
		bucket.Matrix = make([]uint64, bucket.Width*bucket.Depth)
	}
	return bucket
}

// Count adds delta to the count of occurrences of the given key.
// Returns the updated estimated count.
func (r *fnvSketch) Count(key []byte, delta int) uint64 {
	min := uint64(math.MaxUint64)
	k := multihash(key)

	for i := uint(0); i < r.Depth; i++ {
		j := uint(k.hash(i)) % r.Width
		k := i*r.Width + j
		r.Matrix[k] += uint64(delta)
		if v := r.Matrix[k]; v < min {
			min = v
		}
	}

	return min
}

// Query returns the estimated count of the given key.
func (r *fnvSketch) Query(key []byte) uint64 {
	k := multihash(key)
	min := uint64(math.MaxUint64)

	for i := uint(0); i < r.Depth; i++ {
		j := uint(k.hash(i)) % r.Width
		if v := r.Matrix[i*r.Width+j]; v < min {
			min = v
		}
	}

	return min
}
