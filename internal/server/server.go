package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	capi "github.com/hashicorp/consul/api"
	"github.com/mohammednumaan/flux/internal/utils"
)

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

	// the InFlightRequestCount is local to the balancer
	// i.e number of in-flight reqs to this server from the balancer
	InFlightRequestCount int64

	// absolute capacity of this backend (max configured concurrent requests).
	// used for capacity-based normalization: InFlightRequestCount / MaxConfiguredRequestCount.
	MaxConfiguredRequestCount int64

	// i use a percentage because a raw count by itself
	// ignores VOLUME of requests
	RollingErrorRate *RollingErrorRate
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

func requestHandler(serviceName string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		log.Printf("[server]: received request from %s", req.RemoteAddr)
		time.Sleep(5 * time.Second)
		fmt.Fprintf(w, "hello from %s!", serviceName)
	}
}

func healthRequestHandler(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func withUtilization(key string, state *ServerState, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&state.InFlightRequestCount, 1)
		defer atomic.AddInt64(&state.InFlightRequestCount, -1)

		inFlight := atomic.LoadInt64(&state.InFlightRequestCount)
		max := state.MaxConfiguredRequestCount

		var utilization float64
		if max > 0 {
			utilization = float64(inFlight) / float64(max) * 100
		}

		w.Header().Set(key, strconv.FormatFloat(utilization, 'f', 2, 64))
		next.ServeHTTP(w, r)
	})
}

func Start() {

	serverEnv, err := utils.GetServerEnv()
	if err != nil {
		log.Fatalf("[server]: failed to get server env: %v", err)
	}

	serverState := &ServerState{
		InFlightRequestCount:      0,
		MaxConfiguredRequestCount: serverEnv.MaxConfiguredRequestCount,
	}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/api", requestHandler(serverEnv.ServiceName))
		mux.HandleFunc("/health", healthRequestHandler)

		handler := withUtilization("X-Flux-Server-Utilization", serverState, mux)
		log.Printf("[server]: starting server on port %d", serverEnv.ServicePort)

		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", serverEnv.ServicePort), handler))
	}()

	config := capi.DefaultConfig()
	client, err := capi.NewClient(config)
	if err != nil {
		panic(err)
	}

	registration := &capi.AgentServiceRegistration{
		ID:      serverEnv.ServiceID,
		Address: serverEnv.ServiceHost,
		Name:    serverEnv.ServiceName,
		Port:    serverEnv.ServicePort,
		Tags: []string{
			"flux-backend-group=" + serverEnv.BackendGroup,
			"flux-max-requests=" + strconv.FormatInt(serverEnv.MaxConfiguredRequestCount, 10),
		},
		Check: &capi.AgentServiceCheck{
			HTTP:                           fmt.Sprintf("http://%s:%d/health", serverEnv.ServiceHost, serverEnv.ServicePort),
			Interval:                       "10s",
			Timeout:                        "5s",
			DeregisterCriticalServiceAfter: "1m",
		},
	}

	if err := client.Agent().ServiceRegister(registration); err != nil {
		log.Fatalf("[%s] failed to register with consul server agent: %v", serverEnv.ServiceID, err)
	}

	log.Printf("[%s] registered with consul server agent successfully in group %q!", serverEnv.ServiceID, serverEnv.BackendGroup)
	select {}

}
