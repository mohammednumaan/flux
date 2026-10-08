package balancer

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mohammednumaan/flux/internal/types"
)

func createMockServer(host string, port int, handler http.HandlerFunc) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api", handler)
	server := httptest.NewServer(mux)
	return server
}

func TestRouteRequestWithNoServers(t *testing.T) {

	balancer := createBalancer("localhost", 8090)
	balancer.cluster = []*types.Server{}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api", nil)
	balancer.routeRequestHandler(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status code %d, got %d", http.StatusServiceUnavailable, w.Code)
	}
}

func TestScoreServerFavorsLowerServerUtilization(t *testing.T) {
	balancer := createBalancer("localhost", 8090)
	normal := &types.Server{
		InFlightRequestCount:      12,
		MaxConfiguredRequestCount: 100,
		ServerUtilization:         4,
	}
	constrained := &types.Server{
		InFlightRequestCount:      9,
		MaxConfiguredRequestCount: 100,
		ServerUtilization:         31,
	}

	normalScore := balancer.scoreServer(normal)
	constrainedScore := balancer.scoreServer(constrained)

	if normalScore >= constrainedScore {
		t.Fatalf("normal score %f should be lower than constrained score %f", normalScore, constrainedScore)
	}
}

func TestRouteRequestWithServers(t *testing.T) {
	const numServers = 3
	const numRequests = 100

	servers := make([]*types.Server, numServers)
	counters := make([]atomic.Int64, numServers)

	for i := 0; i < numServers; i++ {
		srv := createMockServer("localhost", 8000+i, func(w http.ResponseWriter, r *http.Request) {
			counters[i].Add(1)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		defer srv.Close()

		u, _ := url.Parse(srv.URL)
		port, _ := strconv.Atoi(u.Port())
		servers[i] = &types.Server{
			Host:                 "localhost",
			Port:                 port,
			InFlightRequestCount: 0,
		}
	}

	balancer := createBalancer("localhost", 8090)
	balancer.cluster = servers

	var wg sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/api", nil)
			balancer.routeRequestHandler(w, r)
		}()
	}
	wg.Wait()

	totalServersRouted := 0
	for i := range counters {
		n := counters[i].Load()
		t.Logf("Server %d received %d requests", i, n)
		if n > 0 {
			totalServersRouted++
		}
	}

	if totalServersRouted < 2 {
		t.Errorf("Expected requests to be routed to at least 2 servers, but only routed to %d", totalServersRouted)
	}
}

func TestInFlightRequestCount(t *testing.T) {
	const numServers = 3
	const numRequests = 100

	var barrier sync.WaitGroup
	barrier.Add(numRequests)

	var allRequestsReady sync.WaitGroup
	allRequestsReady.Add(1)

	servers := make([]*types.Server, numServers)
	testServers := make([]*httptest.Server, numServers)

	for i := 0; i < numServers; i++ {
		srv := createMockServer("localhost", 8000+i, func(w http.ResponseWriter, r *http.Request) {
			barrier.Done()
			allRequestsReady.Wait()
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "response from server %d", i)

		})
		testServers[i] = srv
		defer srv.Close()

		u, _ := url.Parse(srv.URL)
		port, _ := strconv.Atoi(u.Port())
		servers[i] = &types.Server{
			Host: "localhost",
			Port: port,
		}
	}

	balancer := createBalancer("localhost", 8090)
	balancer.cluster = servers

	var wg sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/api", nil)
			balancer.routeRequestHandler(w, r)
		}()
	}

	barrier.Wait()
	var total int64
	for _, srv := range servers {
		count := atomic.LoadInt64(&srv.InFlightRequestCount)
		total += count
	}

	if total != numRequests {
		t.Errorf("expected total in-flight %d, got %d", numRequests, total)
	}

	allRequestsReady.Done()
	wg.Wait()
	for i, srv := range servers {
		count := atomic.LoadInt64(&srv.InFlightRequestCount)
		if count != 0 {
			t.Errorf("server %d InFlightRequestCount should be 0 after completion, got %d", i, count)
		}
	}

}

func TestIsAboveThresholdExcludesHighErrorRate(t *testing.T) {
	balancer := createBalancer("localhost", 8090)

	healthyRate := createRollingErrorRate(1 * time.Minute)
	for i := 0; i < 10; i++ {
		healthyRate.Record(false)
	}
	healthy := &types.Server{ServerUtilization: 10, RollingErrorRate: healthyRate}
	if balancer.isAboveThreshold(healthy) {
		t.Errorf("healthy server (rate=%f) should be eligible", healthyRate.Rate())
	}

	poisonedRate := createRollingErrorRate(1 * time.Minute)
	for i := 0; i < 10; i++ {
		poisonedRate.Record(true)
	}
	poisoned := &types.Server{ServerUtilization: 10, RollingErrorRate: poisonedRate}
	if !balancer.isAboveThreshold(poisoned) {
		t.Errorf("poisoned server (rate=%f) should be excluded", poisonedRate.Rate())
	}

	nilRate := &types.Server{ServerUtilization: 10}
	if balancer.isAboveThreshold(nilRate) {
		t.Errorf("server with nil error rate and low utilization should be eligible")
	}

	overloaded := &types.Server{ServerUtilization: 90, RollingErrorRate: healthyRate}
	if !balancer.isAboveThreshold(overloaded) {
		t.Errorf("server above utilization threshold should be excluded")
	}
}

func TestPickServerFallsBackWhenAllPoisoned(t *testing.T) {
	balancer := createBalancer("localhost", 8090)
	for i := 0; i < 3; i++ {
		rate := createRollingErrorRate(1 * time.Minute)
		for j := 0; j < 10; j++ {
			rate.Record(true)
		}
		balancer.cluster = append(balancer.cluster, &types.Server{
			Host: "localhost", Port: 8000 + i,
			ServerUtilization: 10, RollingErrorRate: rate,
		})
	}

	if got := balancer.pickServer(); got == nil {
		t.Errorf("pickServer should fall back to a server instead of nil when all are poisoned")
	}
}
