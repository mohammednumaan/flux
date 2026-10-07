package server

import (
	"math"
	"testing"
	"time"
)

func TestRollingErrorRateRateIgnoresExpiredBuckets(t *testing.T) {
	now := time.Now()
	r := &RollingErrorRate{
		TimeWindow: time.Minute,
		BucketSize: time.Second,
		Buckets: []ErrorRateBucket{
			{Timestamp: now.Add(-10 * time.Second), Total: 90, Errors: 9},
			{Timestamp: now.Add(-2 * time.Minute), Total: 100, Errors: 100},
		},
	}

	got := r.Rate()
	want := 0.1

	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("Rate() = %f, want %f", got, want)
	}
}

func TestRollingErrorRateRecord(t *testing.T) {
	r := &RollingErrorRate{
		TimeWindow: time.Minute,
		BucketSize: time.Second,
		Buckets:    make([]ErrorRateBucket, 60),
	}

	r.Record(false)
	r.Record(true)

	got := r.Rate()
	want := 0.5

	if math.Abs(got-want) > 0.000001 {
		t.Fatalf("Rate() = %f, want %f", got, want)
	}
}

func TestRollingErrorRateDoesNotCountUnusedBuckets(t *testing.T) {
	r := &RollingErrorRate{
		TimeWindow: time.Minute,
		BucketSize: time.Second,
		Buckets:    make([]ErrorRateBucket, 60),
	}

	if got := r.Rate(); got != 0 {
		t.Fatalf("Rate() = %f, want 0 for no requests", got)
	}
}
