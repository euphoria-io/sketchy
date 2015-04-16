package sketchy

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"
)

// Counter provides an interface for tracking the rate at which keys are
// observed.
type RateSketch interface {
	// Count records delta occurrences of key, returning the updated observed
	// rate over the given interval. If interval is smaller than time.Second,
	// or the available data covers less than a second, then 0 is returned.
	Count(key []byte, delta int, interval time.Duration) float64

	// Query returns the observed rate of the given key over the given interval.
	// If interval is smaller than time.Second, or the available data covers
	// less than a second, then 0 is returned.
	Query(key []byte, interval time.Duration) float64
}

type sketchWithTime struct {
	CountSketch *fnvSketch
	Time        time.Time
}

func (b *sketchWithTime) Count(key []byte, delta int) uint64 {
	if b.CountSketch == nil {
		return 0
	}
	return b.CountSketch.Count(key, delta)
}

func (b *sketchWithTime) Query(key []byte) uint64 {
	if b.CountSketch == nil {
		return 0
	}
	return b.CountSketch.Query(key)
}

// RollingCounter maintains a series of count-min sketches to count events in
// time-based buckets. Counts are always applied to the "current" bucket
// (which is reinitialized as needed). Rate queries can use multiple buckets
// to account for the interval of the query.
//
// The interval given to this constructor specifies the maximum duration of
// each bucket. If an event comes in beyond the current bucket's duration,
// then a new bucket is created. If the maximum number of buckets (given
// by num) is exceeded, then the oldest bucket is forgotten.
func RollingCounter(epsilon, delta float64, interval time.Duration, num int) RateSketch {
	return &rollingCounter{
		Epsilon:      epsilon,
		Delta:        delta,
		Interval:     interval,
		NumIntervals: num,
	}
}

type rollingCounter struct {
	Epsilon      float64       // Epsilon parameter for new buckets.
	Delta        float64       // Delta parameter for new buckets.
	Interval     time.Duration // The duration covered by each bucket.
	NumIntervals int           // The maximum number of buckets.

	clock   func() time.Time
	m       sync.Mutex
	buckets []sketchWithTime
}

func (rl *rollingCounter) now() time.Time {
	if rl.clock == nil {
		return time.Now()
	} else {
		return rl.clock()
	}
}

func (rl *rollingCounter) query(
	key []byte, now time.Time, interval time.Duration, latest uint64) (float64, time.Duration) {

	var (
		tc float64
		td time.Duration
	)

	intervalStart := now.Add(-interval)
	for i := len(rl.buckets) - 1; interval > 0 && i >= 0; i-- {
		// figure out how much time the bucket accounts for
		d := now.Sub(rl.buckets[i].Time)
		if d <= 0 {
			continue
		}
		interval -= d

		// determine number of counts in bucket
		var n float64
		if i == len(rl.buckets)-1 && latest != 0 {
			n = float64(latest)
		} else {
			n = float64(rl.buckets[i].Query(key))
		}

		// if our interval begins after this bucket's start time, scale the count
		if intervalStart.After(rl.buckets[i].Time) {
			// d2 is amount of time between interval start and now that is covered
			end := rl.buckets[i].Time.Add(rl.Interval)
			if end.After(now) {
				end = now
			}
			d2 := end.Sub(intervalStart)
			if d-d2 > rl.Interval {
				break
			}
			n = n * float64(d2) / float64(d)
			d = now.Sub(intervalStart)
		}

		tc += n
		td += d
		now = rl.buckets[i].Time
	}
	if td < time.Second {
		return 0, 0
	}
	return tc, td
}

func (rl *rollingCounter) count(key []byte, delta int, now time.Time, interval time.Duration) (
	float64, time.Duration) {

	getWithDefault := func(v, def float64) float64 {
		if v == 0 {
			return def
		}
		return v
	}
	epsilon := getWithDefault(rl.Epsilon, DefaultEpsilon)
	d := getWithDefault(rl.Delta, DefaultDelta)

	if len(rl.buckets) == 0 {
		rl.buckets = []sketchWithTime{
			{
				CountSketch: NewSketch(epsilon, d).(*fnvSketch),
				Time:        now,
			},
		}
	} else if diff := now.Sub(rl.buckets[len(rl.buckets)-1].Time); diff >= rl.Interval {
		newSketch := sketchWithTime{
			CountSketch: NewSketch(epsilon, d).(*fnvSketch),
			Time:        now,
		}
		if len(rl.buckets) >= rl.NumIntervals {
			// shift buckets over by one
			copy(rl.buckets, rl.buckets[1:])
			rl.buckets[len(rl.buckets)-1] = newSketch
		} else {
			rl.buckets = append(rl.buckets, newSketch)
		}
	}

	return rl.query(key, now, interval, rl.buckets[len(rl.buckets)-1].Count(key, delta))
}

// Query returns the observed rate of the given key over the given interval.
// If interval is smaller than time.Second, or the available data covers
// less than a second, then 0 is returned.
func (rl *rollingCounter) Query(key []byte, interval time.Duration) float64 {
	rl.m.Lock()
	defer rl.m.Unlock()

	if len(rl.buckets) == 0 {
		return 0
	}

	tc, d := rl.query(key, rl.now(), interval, 0)
	if d == 0 {
		return 0
	}
	return (tc / float64(d)) * float64(time.Second)
}

// Count records delta occurrences of key, returning the updated observed
// rate over the given interval. If interval is smaller than time.Second,
// or the available data covers less than a second, then 0 is returned.
func (rl *rollingCounter) Count(key []byte, delta int, interval time.Duration) float64 {
	rl.m.Lock()
	defer rl.m.Unlock()

	tc, d := rl.count(key, delta, rl.now(), interval)
	if d == 0 {
		return 0
	}
	return (tc / float64(d)) * float64(time.Second)
}

// GobEncode returns the gob encoding of the current state of the counter.
func (rl *rollingCounter) GobEncode() ([]byte, error) {
	rl.m.Lock()
	defer rl.m.Unlock()

	buf := &bytes.Buffer{}
	encoder := gob.NewEncoder(buf)
	for _, v := range []interface{}{rl.Epsilon, rl.Delta, rl.Interval, rl.NumIntervals, rl.buckets} {
		if err := encoder.Encode(v); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// GobDecode resets the counter to the gob-encoded state provided in data.
func (rl *rollingCounter) GobDecode(data []byte) error {
	rl.m.Lock()
	defer rl.m.Unlock()

	decoder := gob.NewDecoder(bytes.NewReader(data))
	for _, v := range []interface{}{&rl.Epsilon, &rl.Delta, &rl.Interval, &rl.NumIntervals, &rl.buckets} {
		if err := decoder.Decode(v); err != nil {
			return err
		}
	}
	return nil
}

func RollupCounter(epsilon, delta float64, durations ...time.Duration) RateSketch {
	rc := &rollupCounter{Levels: make([]*rollingCounter, len(durations)-1)}
	for i := 1; i < len(durations); i++ {
		from := durations[i-1]
		to := durations[i]
		rc.Levels[i-1] = &rollingCounter{
			Epsilon:      epsilon,
			Delta:        delta,
			Interval:     from,
			NumIntervals: int(to / from),
		}
		if to%from > 0 {
			rc.Levels[i-1].NumIntervals++
		}
	}
	return rc
}

type rollupCounter struct {
	Levels []*rollingCounter
	clock  func() time.Time
}

func (rc *rollupCounter) now() time.Time {
	if rc.clock == nil {
		return time.Now()
	} else {
		return rc.clock()
	}
}

// Query returns the observed rate of the given key over the given interval.
// If interval is smaller than time.Second, or the available data covers
// less than a second, then 0 is returned.
func (rc *rollupCounter) Query(key []byte, interval time.Duration) float64 {
	now := rc.now()
	tc := float64(0)
	td := time.Duration(0)
	for _, c := range rc.Levels {
		if interval <= 0 {
			break
		}
		n, d := c.query(key, now, interval, 0)
		tc += n
		td += d
		now = now.Add(-d)
		interval -= d
	}
	if td == 0 {
		return 0
	}
	return (tc / float64(td)) * float64(time.Second)
}

// Count records delta occurrences of key, returning the updated observed
// rate over the given interval. If interval is smaller than time.Second,
// or the available data covers less than a second, then 0 is returned.
func (rc *rollupCounter) Count(key []byte, delta int, interval time.Duration) float64 {
	now := rc.now()
	tc := float64(0)
	td := time.Duration(0)
	for _, c := range rc.Levels {
		if interval < 0 {
			interval = 0
		}
		n, d := c.count(key, delta, now, interval)
		tc += n
		td += d
		interval -= d
	}
	if td == 0 {
		return 0
	}
	return (tc / float64(td)) * float64(time.Second)
}
