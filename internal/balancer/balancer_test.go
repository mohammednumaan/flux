package balancer

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mohammednumaan/flux/internal/server"
)

func createMockServer(host string, port int, handler http.HandlerFunc) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api", handler)
	server := httptest.NewServer(mux)
	return server
}

func TestRouteRequestWithNoServers(t *testing.T) {

	balancer := createBalancer("localhost", 8090)
	balancer.cluster = []*server.Server{}

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api", nil)
	balancer.routeRequestHandler(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status code %d, got %d", http.StatusServiceUnavailable, w.Code)
	}
}

func TestRouteRequestWithServers(t *testing.T) {
	const numServers = 3
	const numRequests = 100

	servers := make([]*server.Server, numServers)
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
		servers[i] = &server.Server{
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
	for i, srv := range counters {
		n := srv.Load()
		t.Logf("Server %d received %d requests", i, n)
		if n > 0 {
			totalServersRouted++
		}
	}

	if totalServersRouted < 2 {
		t.Errorf("Expected requests to be routed to at least 2 servers, but only routed to %d", totalServersRouted)
	}
}
