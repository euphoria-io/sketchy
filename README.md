[![Build Status](https://travis-ci.org/euphoria-io/sketchy.png)](https://travis-ci.org/euphoria-io/sketchy)
[![GoDoc](https://godoc.org/euphoria.io/sketchy?status.svg)](http://godoc.org/euphoria.io/sketchy)

---

Package sketchy provides count- and rate-tracking sketches. These are probabilistic data structures
that can track the occurrences of a very large number of distinct keys using a relatively small
amount of space. The resulting counts/rates are estimates with a guaranteed level of accuracy.

