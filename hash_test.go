package sketchy

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMultihash(t *testing.T) {
	Convey("FNV-1", t, func() {
		// Test cases taken from hash/fnv.
		golden := map[string]uint64{
			"":    0xcbf29ce484222325,
			"a":   0xaf63bd4c8601b7be,
			"ab":  0x08326707b4eb37b8,
			"abc": 0xd8dcca186bafadcb,
		}
		for k, v := range golden {
			So(v, ShouldEqual, multihash([]byte(k)))
		}
	})
}
