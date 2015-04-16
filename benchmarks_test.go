package sketchy

import (
	"encoding/binary"
	"math/rand"
	"net"
	"runtime"
	"testing"
	"time"
)

// a set of keys to test with
var ips [][]byte

func init() {
	// Precompute the keys we'll choose from.
	for i := 0; i < 50; i++ {
		bytes := make([]byte, 4)
		binary.BigEndian.PutUint32(bytes, rand.Uint32())
		ip := []byte(net.IP(bytes).String())
		for j := 0; j < i; j++ {
			ips = append(ips, ip)
		}
	}
}

type event struct {
	ip []byte
	ts time.Time
}

func BenchmarkSketch(b *testing.B) {
	b.StopTimer()
	runtime.GC()

	// Precompute the choices we'll make.
	indexes := make([]int, b.N)
	for i := 0; i < b.N; i++ {
		indexes[i] = rand.Intn(len(ips))
	}

	// Initialize counter and info state.
	bucket := NewSketch(0.9957, 0.993)
	maxIP := []byte{}
	maxIPCount := uint64(0)

	// Run the benchmark.
	b.StartTimer()
	for _, i := range indexes {
		ip := ips[i]
		if v := bucket.Count(ip, 1); v > maxIPCount {
			maxIPCount = v
			maxIP = ip
		}
	}
	b.StopTimer()
	b.Logf("max IP for n=%d: %s (%d)", b.N, string(maxIP), maxIPCount)
	runtime.GC()
}

func BenchmarkRollingCounter(b *testing.B) {
	b.StopTimer()
	runtime.GC()

	// Precompute the events we'll track.
	mean := float64(b.N) / float64(24*time.Hour)
	events := make([]event, b.N)
	ts := time.Now()
	for i := 0; i < b.N; i++ {
		events[i].ip = ips[rand.Intn(len(ips))]
		ts = ts.Add(time.Duration(rand.ExpFloat64() / mean))
		events[i].ts = ts
	}

	// Initialize counter and info state.
	counter := RollingCounter(0, 0, 5*time.Minute, 12).(*rollingCounter)
	counter.clock = func() time.Time { return ts }
	maxIP := []byte{}
	maxIPRate := 0.0

	// Run the benchmark.
	b.StartTimer()
	for _, e := range events {
		ts = e.ts
		if rate := counter.Count(e.ip, 1, time.Minute); rate > maxIPRate {
			maxIPRate = rate
			maxIP = e.ip
		}
	}
	b.StopTimer()
	b.Logf("max IP for n=%d: %s (%f)", b.N, string(maxIP), maxIPRate)
	runtime.GC()
}

func BenchmarkRollupCounter(b *testing.B) {
	b.StopTimer()
	runtime.GC()

	// Precompute the events we'll track.
	mean := float64(b.N) / float64(24*time.Hour)
	events := make([]event, b.N)
	ts := time.Now()
	for i := 0; i < b.N; i++ {
		events[i].ip = ips[rand.Intn(len(ips))]
		ts = ts.Add(time.Duration(rand.ExpFloat64() / mean))
		events[i].ts = ts
	}

	// Initialize counter and info state.
	counter := RollupCounter(0, 0, 15*time.Minute, time.Hour, 4*time.Hour, 24*time.Hour).(*rollupCounter)
	counter.clock = func() time.Time { return ts }
	maxIP := []byte{}
	maxIPRate := 0.0

	// Run the benchmark.
	b.StartTimer()
	for _, e := range events {
		ts = e.ts
		if rate := counter.Count(e.ip, 1, time.Hour); rate > maxIPRate {
			maxIPRate = rate
			maxIP = e.ip
		}
	}
	b.StopTimer()
	b.Logf("max IP for n=%d: %s (%f)", b.N, string(maxIP), maxIPRate)
	runtime.GC()
}
