package sketchy

// Constants and algorithm taken from hash/fnv.

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

type hashKernel uint64

func (k hashKernel) hash(index uint) uint64 {
	v := uint64(k)
	return (v & 0xffffffff) + (v>>32)*uint64(index)
}

func multihash(key []byte) hashKernel {
	k := uint64(fnvOffset64)
	for _, b := range key {
		k *= fnvPrime64
		k ^= uint64(b)
	}
	return hashKernel(k)
}
