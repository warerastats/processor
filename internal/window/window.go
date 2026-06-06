// Package window provides UTC boundary math shared by windowed processor jobs.
package window

import "time"

// FloorUTC truncates t to the start of its size-aligned window in UTC.
func FloorUTC(t time.Time, size time.Duration) time.Time {
	t = t.UTC()
	return t.Truncate(size)
}

// ClosedBoundary returns the start of the most recent fully-closed window of
// the given size as of now: the largest size-aligned instant <= now-size.
func ClosedBoundary(now time.Time, size time.Duration) time.Time {
	return FloorUTC(now.UTC().Add(-size), size)
}

// Each iterates every size-aligned window start in [from, until], inclusive of
// from and until when aligned, calling fn with each window's start. from and
// until are floored to the window grid first. It is a no-op when until < from.
func Each(from, until time.Time, size time.Duration, fn func(start time.Time)) {
	from = FloorUTC(from, size)
	until = FloorUTC(until, size)
	for t := from; !t.After(until); t = t.Add(size) {
		fn(t)
	}
}

// NextAligned returns the first size-aligned instant strictly after t.
func NextAligned(t time.Time, size time.Duration) time.Time {
	return FloorUTC(t, size).Add(size)
}
