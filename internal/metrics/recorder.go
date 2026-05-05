// Package metrics records counts and latency distributions for benchmarks.
//
// A Recorder is safe for concurrent use. Snapshot returns deltas over a
// configurable interval so callers can print rolling RPS / percentile lines.
package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	// Histogram covers 1ns .. 60s with 3 significant figures.
	histMinValue      = 1
	histMaxValue      = int64(time.Minute)
	histSigFigs       = 3
)

type Recorder struct {
	count  atomic.Int64
	failed atomic.Int64
	bytes  atomic.Int64

	mu   sync.Mutex
	hist *hdr.Histogram
}

func New() *Recorder {
	return &Recorder{hist: hdr.New(histMinValue, histMaxValue, histSigFigs)}
}

func (r *Recorder) Inc(bytes int) {
	r.count.Add(1)
	r.bytes.Add(int64(bytes))
}

func (r *Recorder) Fail() { r.failed.Add(1) }

func (r *Recorder) Observe(d time.Duration) {
	if d < time.Duration(histMinValue) {
		d = time.Duration(histMinValue)
	}
	if d > time.Duration(histMaxValue) {
		d = time.Duration(histMaxValue)
	}
	r.mu.Lock()
	_ = r.hist.RecordValue(int64(d))
	r.mu.Unlock()
}

type Snapshot struct {
	Count, Failed, Bytes int64
	P50, P95, P99, Max   time.Duration
}

// snapshot returns the current totals; if reset is true the latency histogram
// is cleared so the next snapshot reflects only the next interval.
func (r *Recorder) snapshot(reset bool) Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Snapshot{
		Count:  r.count.Load(),
		Failed: r.failed.Load(),
		Bytes:  r.bytes.Load(),
		P50:    time.Duration(r.hist.ValueAtQuantile(50)),
		P95:    time.Duration(r.hist.ValueAtQuantile(95)),
		P99:    time.Duration(r.hist.ValueAtQuantile(99)),
		Max:    time.Duration(r.hist.Max()),
	}
	if reset {
		r.hist.Reset()
	}
	return s
}

// Reporter prints a delta-based RPS / latency line on each Tick call.
type Reporter struct {
	r          *Recorder
	prevCount  int64
	prevFailed int64
	prevBytes  int64
	last       time.Time
}

func NewReporter(r *Recorder) *Reporter {
	return &Reporter{r: r, last: time.Now()}
}

func (rp *Reporter) Tick(label string) string {
	now := time.Now()
	dt := now.Sub(rp.last).Seconds()
	if dt <= 0 {
		dt = 1
	}
	s := rp.r.snapshot(true)

	dc := s.Count - rp.prevCount
	df := s.Failed - rp.prevFailed
	db := s.Bytes - rp.prevBytes
	rp.prevCount, rp.prevFailed, rp.prevBytes, rp.last = s.Count, s.Failed, s.Bytes, now

	return fmt.Sprintf(
		"%s rps=%.0f fail/s=%.0f mb/s=%.2f p50=%s p95=%s p99=%s max=%s total=%d failed=%d",
		label,
		float64(dc)/dt,
		float64(df)/dt,
		float64(db)/dt/(1<<20),
		s.P50.Round(time.Microsecond),
		s.P95.Round(time.Microsecond),
		s.P99.Round(time.Microsecond),
		s.Max.Round(time.Microsecond),
		s.Count,
		s.Failed,
	)
}
