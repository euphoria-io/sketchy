package sketchy

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"math/rand"
	"net"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func encode(v interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(v interface{}, encoding []byte) error {
	return gob.NewDecoder(bytes.NewReader(encoding)).Decode(v)
}

func TestRollingCounter(t *testing.T) {
	now := time.Now()
	key := []byte("key")

	Convey("Query", t, func() {
		counter := RollingCounter(0, 0, time.Minute, 3).(*rollingCounter)
		counter.clock = func() time.Time { return now }

		So(counter.Query(key, 90*time.Second), ShouldEqual, 0)

		counter.buckets = []sketchWithTime{
			{
				CountSketch: NewSketch(0, 0).(*fnvSketch),
				Time:        now,
			},
		}

		now = now.Add(30 * time.Second)
		So(counter.Query(key, 15*time.Second), ShouldEqual, 0)
		counter.buckets[0].Count(key, 60)
		So(counter.Query(key, 15*time.Second), ShouldEqual, 2.0)
		now = now.Add(30 * time.Second)
		So(counter.Query(key, 90*time.Second), ShouldEqual, 1.0)

		counter.buckets = append(counter.buckets, sketchWithTime{
			CountSketch: NewSketch(0, 0).(*fnvSketch),
			Time:        now,
		})
		So(counter.Query(key, 90*time.Second), ShouldEqual, 1.0)
		counter.buckets[1].Count(key, 30)
		So(counter.Query(key, 90*time.Second), ShouldEqual, 1.0)
		now = now.Add(time.Second)
		So(counter.Query(key, 90*time.Second), ShouldEqual, 90.0/61)
		now = now.Add(29 * time.Second)
		So(counter.Query(key, 90*time.Second), ShouldEqual, 1.0)
		now = now.Add(30 * time.Second)
		So(counter.Query(key, 90*time.Second), ShouldEqual, 2.0/3.0)
	})

	Convey("Single key", t, func() {
		counter := RollingCounter(0, 0, 60*time.Second, 10).(*rollingCounter)
		counter.clock = func() time.Time { return now }

		counter.Count(key, 1, 0)
		now = now.Add(time.Second)
		So(counter.Query(key, 60*time.Second), ShouldEqual, 1.0)

		counter.Count(key, 479, 0)
		now = now.Add(59 * time.Second)
		So(counter.Query(key, 60*time.Second), ShouldEqual, 8.0)

		now = now.Add(time.Second)
		counter.Count(key, 240, 0)
		now = now.Add(60 * time.Second)
		So(counter.Query(key, 60*time.Second), ShouldEqual, 4.0)
		So(counter.Query(key, 120*time.Second), ShouldAlmostEqual, (480.0*59.0/61.0+240.0)/120.0)

		now = now.Add(time.Second)
		counter.Count(key, 120, 0)
		now = now.Add(60 * time.Second)
		So(counter.Query(key, 60*time.Second), ShouldEqual, 2.0)
		So(counter.Query(key, 120*time.Second), ShouldAlmostEqual,
			(240.0*59.0/61.0+120.0)/120.0)
		So(counter.Query(key, 180*time.Second), ShouldAlmostEqual,
			(480.0*58.0/61.0+240.0+120.0)/180.0)

		now = now.Add(1 * time.Second)
		So(counter.Count(key, 1, time.Second), ShouldEqual, 0)
		So(counter.Query(key, 60*time.Second), ShouldAlmostEqual, ((59.0/61)*120)/60)
		So(counter.Query(key, 120*time.Second), ShouldAlmostEqual, ((58.0/61)*240+120)/120)
		So(counter.Query(key, 180*time.Second), ShouldAlmostEqual, ((57.0/61)*480+240+120)/180)
		So(counter.Query(key, 300*time.Second), ShouldAlmostEqual, (480.0+240.0+120.0)/183.0)
	})

	Convey("Sparse rate", t, func() {
		counter := RollingCounter(0, 0, 60*time.Second, 10).(*rollingCounter)
		counter.clock = func() time.Time { return now }

		counter.Count(key, 1, 0)
		now = now.Add(598 * time.Second)
		So(counter.Count(key, 1, 600*time.Second), ShouldAlmostEqual, 1.0/598)
		now = now.Add(time.Second)
		So(counter.Query(key, 600*time.Second), ShouldAlmostEqual, 2.0/599)

		now = now.Add(599 * time.Second)
		So(counter.Count(key, 1, 600*time.Second), ShouldAlmostEqual, 1.0/600)
		now = now.Add(time.Second)
		So(counter.Query(key, 600*time.Second), ShouldEqual, 1.0)

		now = now.Add(1200 * time.Second)
		So(counter.Count(key, 1, 600*time.Second), ShouldAlmostEqual, 0)
	})

	Convey("Intermittent rate", t, func() {
		counter := RollingCounter(0, 0, 60*time.Second, 10).(*rollingCounter)
		counter.clock = func() time.Time { return now }

		for i := 0; i < 10; i++ {
			counter.Count(key, 1, 0)
			now = now.Add(60 * time.Second)
		}

		now = now.Add(420 * time.Second)
		counter.Count(key, 1, 0)
		now = now.Add(60 * time.Second)
		So(counter.Query(key, 600*time.Second), ShouldAlmostEqual, 3./600)
		now = now.Add(359 * time.Second)
		So(counter.Query(key, 600*time.Second), ShouldAlmostEqual, 1./419.0)
	})

	Convey("Gob encoding/decoding should result in the same rates", t, func() {
		n := 500
		events := make([][]byte, 0, (n*n+n)/2)
		counts := map[string]uint64{}
		firstIP := []byte{}
		lastIP := []byte{}
		for len(events) < cap(events) {
			bytes := make([]byte, 4)
			binary.BigEndian.PutUint32(bytes, rand.Uint32())
			ip := []byte(net.IP(bytes).String())
			if firstIP == nil {
				firstIP = ip
			}
			lastIP = ip
			if _, ok := counts[string(ip)]; !ok {
				counts[string(ip)] = uint64(len(counts))
				for i := uint64(0); i < counts[string(ip)]; i++ {
					events = append(events, ip)
				}
			}
		}

		counter := RollingCounter(0, 0, 300*time.Second, 12).(*rollingCounter)
		counter.clock = func() time.Time { return now }

		// simulate events
		// mean rate should be n events per 10 minutes
		mean := float64(len(events)) / (float64(counter.NumIntervals) * float64(counter.Interval))
		for _, i := range rand.Perm(len(events)) {
			r := rand.ExpFloat64()
			delay := time.Duration(r / mean)
			now = now.Add(delay)
			counter.Count(events[i], 1, 0)
		}

		heaviestRate30s := counter.Query(lastIP, 30*time.Second)
		heaviestRate10m := counter.Query(lastIP, 10*time.Minute)
		Printf("30s rate of busiest IP: %f\n", heaviestRate30s)
		Printf("10m rate of busiest IP: %f\n", heaviestRate10m)

		lightestRate30s := counter.Query(firstIP, 30*time.Second)
		lightestRate10m := counter.Query(firstIP, 10*time.Minute)
		Printf("30s rate of lightest IP: %f\n", lightestRate30s)
		Printf("10m rate of lightest IP: %f\n", lightestRate10m)

		encoding, err := encode(counter)
		So(err, ShouldBeNil)
		Printf("encoding is %d bytes\n", len(encoding))

		clone := &rollingCounter{clock: counter.clock}
		So(decode(clone, encoding), ShouldBeNil)

		So(clone.Query(lastIP, 30*time.Second), ShouldEqual, heaviestRate30s)
		So(clone.Query(lastIP, 10*time.Minute), ShouldEqual, heaviestRate10m)

		So(counter.Query(firstIP, 30*time.Second), ShouldEqual, lightestRate30s)
		So(counter.Query(firstIP, 10*time.Minute), ShouldEqual, lightestRate10m)
	})

	Convey("Going back in time", t, func() {
		counter := RollingCounter(0, 0, 60*time.Second, 10).(*rollingCounter)
		counter.clock = func() time.Time { return now }
		for i := 0; i < 10; i++ {
			now = now.Add(time.Minute)
			counter.Count(key, i+1, 0)
		}
		n, d := counter.query(key, now.Add(-4*time.Minute), 90*time.Second, 0)
		So(n, ShouldEqual, 7)
		So(d, ShouldEqual, 90*time.Second)
	})
}

func TestRollupCounter(t *testing.T) {
	key := []byte("key")
	now := time.Now()

	Convey("Rolling up", t, func() {
		rollup := RollupCounter(
			0, 0, 10*time.Minute, time.Hour, 6*time.Hour, 24*time.Hour).(*rollupCounter)
		rollup.clock = func() time.Time { return now }
		So(rollup.Count(key, 1, time.Second), ShouldEqual, 0)
		now = now.Add(time.Second)
		So(rollup.Count(key, 1, time.Second), ShouldEqual, 2)

		now = now.Add(10 * time.Minute)
		So(rollup.Count(key, 1, time.Minute), ShouldAlmostEqual, (2*59./601)/60)
		now = now.Add(time.Second)
		So(rollup.Query(key, time.Minute), ShouldAlmostEqual, (2*58./601+1)/60)

		now = now.Add(time.Hour)
		So(rollup.Query(key, time.Minute), ShouldAlmostEqual, 3.0/4202)

		now = now.Add(5 * time.Minute)
		So(rollup.Count(key, 1, time.Hour), ShouldAlmostEqual, (3*(7200.0-4502.0)/4502)/3600)
	})

	Convey("Gob encoding/decoding should result in the same rates", t, func() {
		n := 500
		events := make([][]byte, 0, (n*n+n)/2)
		counts := map[string]uint64{}
		firstIP := []byte{}
		lastIP := []byte{}
		for len(events) < cap(events) {
			bytes := make([]byte, 4)
			binary.BigEndian.PutUint32(bytes, rand.Uint32())
			ip := []byte(net.IP(bytes).String())
			if firstIP == nil {
				firstIP = ip
			}
			lastIP = ip
			if _, ok := counts[string(ip)]; !ok {
				counts[string(ip)] = uint64(len(counts))
				for i := uint64(0); i < counts[string(ip)]; i++ {
					events = append(events, ip)
				}
			}
		}

		counter := RollupCounter(
			0, 0, 15*time.Minute, time.Hour, 4*time.Hour, 24*time.Hour).(*rollupCounter)
		counter.clock = func() time.Time { return now }

		// simulate events
		// mean rate should be n events per 10 minutes
		mean := float64(len(events)) / float64(24*time.Hour)
		for _, i := range rand.Perm(len(events)) {
			r := rand.ExpFloat64()
			delay := time.Duration(r / mean)
			now = now.Add(delay)
			counter.Count(events[i], 1, 0)
		}

		heaviestRate30s := counter.Query(lastIP, 30*time.Second)
		heaviestRate10m := counter.Query(lastIP, 10*time.Minute)
		Printf("30s rate of busiest IP: %f\n", heaviestRate30s)
		Printf("10m rate of busiest IP: %f\n", heaviestRate10m)

		lightestRate30s := counter.Query(firstIP, 30*time.Second)
		lightestRate10m := counter.Query(firstIP, 10*time.Minute)
		Printf("30s rate of lightest IP: %f\n", lightestRate30s)
		Printf("10m rate of lightest IP: %f\n", lightestRate10m)

		encoding, err := encode(counter)
		So(err, ShouldBeNil)
		Printf("encoding is %d bytes\n", len(encoding))

		clone := &rollupCounter{clock: counter.clock}
		So(decode(clone, encoding), ShouldBeNil)

		So(clone.Query(lastIP, 30*time.Second), ShouldEqual, heaviestRate30s)
		So(clone.Query(lastIP, 10*time.Minute), ShouldEqual, heaviestRate10m)

		So(counter.Query(firstIP, 30*time.Second), ShouldEqual, lightestRate30s)
		So(counter.Query(firstIP, 10*time.Minute), ShouldEqual, lightestRate10m)
	})
}
