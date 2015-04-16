package sketchy

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"net"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func shouldEqual(actual interface{}, expected ...interface{}) string {
	actualCounts := actual.(map[string]uint64)
	expectedCounts := expected[0].(map[string]uint64)
	epsilon := expected[1].(float64)
	delta := expected[2].(float64)

	tot := uint64(0)
	for _, v := range expectedCounts {
		tot += v
	}

	errs := 0
	for k, v := range expectedCounts {
		diff := math.Abs(float64(v) - float64(actualCounts[k]))
		if diff > math.Ceil(epsilon*float64(tot)) {
			Printf("error of %f (%f)\n", diff-math.Ceil(epsilon*float64(actualCounts[k])),
				float64(diff)/float64(tot))
			errs++
		}
	}

	if max := delta * float64(len(expectedCounts)); float64(errs) > max {
		return fmt.Sprintf("more than %d counts (%d) were outside of epsilon range", int(max), errs)
	}

	return ""
}

func TestSketch(t *testing.T) {
	counts := map[string]uint64{
		"one":           1,
		"two":           2,
		"three":         3,
		"a lot":         42,
		"a bunch":       512,
		"tons":          1024,
		"wow much spam": 64000,
	}

	for len(counts) < 500 {
		bytes := make([]byte, 4)
		binary.BigEndian.PutUint32(bytes, rand.Uint32())
		ip := net.IP(bytes).String()
		if _, ok := counts[ip]; !ok {
			counts[ip] = uint64(len(counts))
		}
	}

	tot := uint64(0)
	for _, v := range counts {
		tot += v
	}
	events := make([]string, 0, tot)
	for k, v := range counts {
		for i := uint64(0); i < v; i++ {
			events = append(events, k)
		}
	}

	latest := map[string]uint64{}
	bucket := NewSketch(0, 0)
	for _, i := range rand.Perm(len(events)) {
		latest[events[i]] = bucket.Count([]byte(events[i]), 1)
	}

	Convey("Counts should be roughly accurate", t, func() {
		So(latest, shouldEqual, counts, 0.001, 0.01)
	})

	Convey("Gob encoding/decoding should result in the same counts", t, func() {
		encoding, err := encode(bucket)
		So(err, ShouldBeNil)
		Printf("encoding is %d bytes\n", len(encoding))

		clone := NewSketch(0, 0)
		So(decode(clone, encoding), ShouldBeNil)

		cloneCounts := map[string]uint64{}
		for k, v := range counts {
			cloneCounts[k] = v + 1
			latest[k] = clone.Count([]byte(k), 1)
		}
		So(latest, shouldEqual, cloneCounts, 0.001, 0.01)
	})

	Convey("Count returns the updated value", t, func() {
		bucket := NewSketch(0, 0)
		So(bucket.Count([]byte("key"), 10), ShouldEqual, 10)
	})
}
