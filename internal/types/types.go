package types

import (
	"sync"
	"time"
)

type ServerEnv struct {
	ConsulHttpAddr            string
	ServiceID                 string
	ServiceName               string
	ServiceHost               string
	ServicePort               int
	MaxConfiguredRequestCount int64
	BackendGroup              string
}

type BalancerEnv struct {
	ConsulHttpAddr    string
	ServiceName       string
	ServiceHost       string
	ServicePort       int
	TargetServiceName string
}

/*
here Server and ServerState are different structs, they are completely independent.
Server is used to represent a server in the cluster/pool that the balancer TRACKS.
ServerState is used to represent the server utilization state that the server itself reports to the balancer.
*/

type Server struct {
	Host              string
	Port              int
	BackendGroup      string
	ServerUtilization float64

	InFlightRequestCount      int64
	MaxConfiguredRequestCount int64
	RollingErrorRate          *RollingErrorRate
}

type RollingErrorRate struct {
	TimeWindow time.Duration
	BucketSize time.Duration
	Buckets    []ErrorRateBucket
	mu         sync.Mutex
}

type ErrorRateBucket struct {
	Timestamp time.Time
	Total     int64
	Errors    int64
}

func (r *RollingErrorRate) Record(failed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.BucketSize <= 0 || len(r.Buckets) == 0 {
		return
	}

	now := time.Now()
	bucketTime := now.Truncate(r.BucketSize)
	bucketIdx := int(
		(bucketTime.UnixNano() / int64(r.BucketSize)) % int64(len(r.Buckets)),
	)

	bucket := &r.Buckets[bucketIdx]
	if !bucket.Timestamp.Equal(bucketTime) {
		bucket.Timestamp = bucketTime
		bucket.Total = 0
		bucket.Errors = 0
	}

	bucket.Total++
	if failed {
		bucket.Errors++
	}
}

func (r *RollingErrorRate) Rate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoff := time.Now().Add(-r.TimeWindow)
	var total int64
	var errors int64

	for i := range r.Buckets {
		bucket := &r.Buckets[i]
		if bucket.Timestamp.IsZero() || bucket.Timestamp.Before(cutoff) {
			continue
		}

		total += bucket.Total
		errors += bucket.Errors
	}

	if total == 0 {
		return 0
	}

	return float64(errors) / float64(total)
}

type ServerState struct {
	InFlightRequestCount      int64
	MaxConfiguredRequestCount int64
}
