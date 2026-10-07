package balancer

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	capi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/mohammednumaan/flux/internal/server"
	"github.com/mohammednumaan/flux/internal/utils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	backendGroupTag               = "flux-backend-group="
	backendMaxRequestsTag         = "flux-max-requests="
	inFlightRequestCountWeight    = 0.5
	serverUtilizationWeight       = 0.4
	rollingErrorRateWeight        = 0.1
	maxServerUtilizationThreshold = 85.0
	filterMaxAttempts             = 5
)

var metricsRegistry = prometheus.NewRegistry()
var requestsTotal = promauto.With(metricsRegistry).NewCounterVec(
	prometheus.CounterOpts{
		Name: "balancer_requests_total",
		Help: "Total number of requests routed by the balancer",
	},
	[]string{"backend", "backend_group"},
)
var serverInFlight = promauto.With(metricsRegistry).NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "balancer_server_inflight_requests",
		Help: "Number of in-flight requests to each backend server",
	},
	[]string{"backend", "backend_group"},
)

var serverUtilization = promauto.With(metricsRegistry).NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "balancer_server_utilization",
		Help: "Utilization of each backend server",
	},
	[]string{"backend", "backend_group"},
)

type BalancerState struct {
	host    string
	port    int
	cluster []*server.Server
	mu      sync.Mutex
}

func (b *BalancerState) isAboveThreshold(srv *server.Server) bool {
	if srv == nil {
		return false
	}

	return srv.ServerUtilization <= maxServerUtilizationThreshold
}

func (b *BalancerState) pickOneFiltered(excludeIdx int) (*server.Server, int) {
	n := len(b.cluster)
	for i := 0; i < filterMaxAttempts; i++ {
		idx := rand.IntN(n)
		if idx == excludeIdx {
			continue
		}
		srv := b.cluster[idx]

		if b.isAboveThreshold(srv) {
			return srv, idx
		}
	}

	for {
		idx := rand.IntN(n)
		if idx != excludeIdx {
			srv := b.cluster[idx]
			return srv, idx
		}
	}

}

func clamp(val, min, max float64) float64 {
	if val < min {
		return min
	}

	if val > max {
		return max
	}

	return val
}

func (b *BalancerState) scoreServer(srv *server.Server) float64 {
	var normInFlight float64

	if srv.MaxConfiguredRequestCount > 0 {
		// the reason i normalize this by max configured request count
		// is because consider this scenario:
		// server a: 8 inflight, max 10
		// server b: 8 inflight, max 100
		// if we normalize using max inflight (local view), both servers will have the
		// same score, but server a is more loaded than server b, so we should prefer server b.
		normInFlight = float64(srv.InFlightRequestCount) / float64(srv.MaxConfiguredRequestCount)
	}

	normUtilization := clamp(srv.ServerUtilization, 0, 100) / 100
	var normErrorRate float64
	if srv.RollingErrorRate != nil {
		normErrorRate = clamp(srv.RollingErrorRate.Rate(), 0, 1)
	}

	return inFlightRequestCountWeight*normInFlight +
		serverUtilizationWeight*normUtilization +
		rollingErrorRateWeight*normErrorRate

}

func (b *BalancerState) pickServer() *server.Server {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(b.cluster)
	if n == 0 {
		return nil
	}

	selected := b.cluster[0]
	if n >= 2 {
		candidate1, idx := b.pickOneFiltered(-1)
		candidate2, _ := b.pickOneFiltered(idx)

		score1 := b.scoreServer(candidate1)
		score2 := b.scoreServer(candidate2)

		if score1 < score2 {
			selected = candidate1
		} else {
			selected = candidate2
		}
	}

	selected.InFlightRequestCount++
	backend := fmt.Sprintf("%s:%d", selected.Host, selected.Port)
	serverInFlight.WithLabelValues(backend, selected.BackendGroup).Set(float64(selected.InFlightRequestCount))
	return selected
}

func (b *BalancerState) releaseServer(srv *server.Server) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if srv.InFlightRequestCount > 0 {
		srv.InFlightRequestCount--
	}

	backend := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
	serverInFlight.WithLabelValues(backend, srv.BackendGroup).Set(float64(srv.InFlightRequestCount))
}

func (b *BalancerState) UpdateServerUtilization(srv *server.Server, utilization float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	srv.ServerUtilization = utilization

	backend := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
	serverUtilization.WithLabelValues(backend, srv.BackendGroup).Set(utilization)
}

func createRollingErrorRate(timeWindow time.Duration) *server.RollingErrorRate {
	bucketSize := 1 * time.Second
	bucketCount := int(timeWindow / bucketSize)

	return &server.RollingErrorRate{
		TimeWindow: timeWindow,
		BucketSize: bucketSize,
		Buckets:    make([]server.ErrorRateBucket, bucketCount),
	}
}

func createServer(host string, port int, backendGroup string, maxRequests int64) *server.Server {
	return &server.Server{
		Host:                      host,
		Port:                      port,
		BackendGroup:              backendGroup,
		ServerUtilization:         0.0,
		InFlightRequestCount:      0,
		MaxConfiguredRequestCount: maxRequests,
		RollingErrorRate:          createRollingErrorRate(1 * time.Minute),
	}
}

func groupFromTags(tags []string) string {
	for _, tag := range tags {
		if strings.HasPrefix(tag, backendGroupTag) {
			return strings.TrimPrefix(tag, backendGroupTag)
		}
	}

	return "unknown"
}

func maxRequestsFromTags(tags []string) int64 {
	for _, tag := range tags {
		if strings.HasPrefix(tag, backendMaxRequestsTag) {
			v := strings.TrimPrefix(tag, backendMaxRequestsTag)
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
				return n
			}
		}
	}

	return 0
}

func createBalancer(host string, port int) *BalancerState {
	return &BalancerState{
		host:    host,
		port:    port,
		cluster: make([]*server.Server, 0),
	}
}

func balancerWatchHandler(b *BalancerState) func(blockParam watch.BlockingParamVal, data interface{}) {
	return func(blockParam watch.BlockingParamVal, data interface{}) {
		if data == nil {
			log.Println("[balancer]: no data received from consul watch")
			return
		}

		services, ok := data.([]*capi.ServiceEntry)
		if !ok {
			log.Fatalf("[balancer]: failed to cast data to []*capi.ServiceEntry")
			return
		}

		b.mu.Lock()
		existing := make(map[string]*server.Server)
		for _, srv := range b.cluster {
			hostPort := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
			existing[hostPort] = srv
		}

		b.mu.Unlock()
		newCluster := make([]*server.Server, 0)
		for _, entry := range services {
			healthy := true
			for _, check := range entry.Checks {
				if check.Status != capi.HealthPassing {
					healthy = false
					break
				}
			}

			if !healthy {
				log.Printf("[balancer]: server %s:%d is not healthy, skipping", entry.Service.Address, entry.Service.Port)
				continue
			}

			hostPort := fmt.Sprintf("%s:%d", entry.Service.Address, entry.Service.Port)
			srv, exists := existing[hostPort]
			if !exists {
				srv = createServer(entry.Service.Address, entry.Service.Port, groupFromTags(entry.Service.Tags), maxRequestsFromTags(entry.Service.Tags))
			} else if cap := maxRequestsFromTags(entry.Service.Tags); cap > 0 {
				srv.MaxConfiguredRequestCount = cap
			}

			newCluster = append(newCluster, srv)
		}

		b.mu.Lock()
		b.cluster = newCluster
		b.mu.Unlock()

		for _, server := range b.cluster {
			log.Printf("[balancer]: registered server %s:%d in group %q", server.Host, server.Port, server.BackendGroup)
		}

	}
}

func (b *BalancerState) routeRequestHandler(w http.ResponseWriter, req *http.Request) {
	selected := b.pickServer()
	if selected == nil {
		http.Error(w, "No available servers", http.StatusServiceUnavailable)
		return
	}

	defer b.releaseServer(selected)

	backend := fmt.Sprintf("%s:%d", selected.Host, selected.Port)
	requestsTotal.WithLabelValues(backend, selected.BackendGroup).Inc()
	statusCode, err := ForwardRequest(b, selected, w, req)
	failed := err != nil || statusCode >= http.StatusInternalServerError
	if selected.RollingErrorRate != nil {
		selected.RollingErrorRate.Record(failed)
	}

	if err != nil {
		log.Printf("[balancer]: failed to forward request to server %s:%d: %v", selected.Host, selected.Port, err)
		http.Error(w, "Failed to forward request", http.StatusInternalServerError)
		return
	}

}

func Start() {
	balancerEnv, err := utils.GetBalancerEnv()
	if err != nil {
		log.Fatalf("[balancer]: failed to get balancer env: %v", err)
	}

	balancer := createBalancer(balancerEnv.ServiceHost, balancerEnv.ServicePort)

	go func() {
		http.HandleFunc("/api", balancer.routeRequestHandler)

		http.Handle(
			"/metrics",
			promhttp.HandlerFor(
				metricsRegistry,
				promhttp.HandlerOpts{},
			),
		)
		log.Printf("[balancer]: starting balancer at port :%d", balancerEnv.ServicePort)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", balancerEnv.ServicePort), nil))
	}()

	watchConfig := map[string]interface{}{
		"type":        "service",
		"service":     balancerEnv.TargetServiceName,
		"passingonly": true,
	}

	plan, err := watch.Parse(watchConfig)
	if err != nil {
		log.Fatalf("[balancer]: failed to create watch plan in consul: %v", err)
	}

	plan.HybridHandler = balancerWatchHandler(balancer)
	fmt.Println("[balancer]: starting consul watch for flux-backend service")

	if err := plan.Run(balancerEnv.ConsulHttpAddr); err != nil {
		log.Fatalf("[balancer]: failed to run watch plan in consul: %v", err)
	}

	select {}

}
