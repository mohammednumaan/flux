package balancer

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"

	capi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/mohammednumaan/flux/internal/server"
	"github.com/mohammednumaan/flux/internal/utils"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	inFlightWeight   = 0.6
	serverUtilWeight = 0.4
)

var metricsRegistry = prometheus.NewRegistry()
var requestsTotal = promauto.With(metricsRegistry).NewCounterVec(
	prometheus.CounterOpts{
		Name: "balancer_requests_total",
		Help: "Total number of requests routed by the balancer",
	},
	[]string{"backend"},
)
var serverInFlight = promauto.With(metricsRegistry).NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "balancer_server_inflight_requests",
		Help: "Number of in-flight requests to each backend server",
	},
	[]string{"backend"},
)

var serverUtilization = promauto.With(metricsRegistry).NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "balancer_server_utilization",
		Help: "Utilization of each backend server",
	},
	[]string{"backend"},
)

type BalancerState struct {
	host    string
	port    int
	cluster []*server.Server
	current int
	mu      sync.Mutex
}

func (b *BalancerState) pickServer() *server.Server {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(b.cluster)
	if n == 0 {
		return nil
	}

	maxInflight := int64(0)
	for _, srv := range b.cluster {
		if srv.InFlightRequestCount > maxInflight {
			maxInflight = srv.InFlightRequestCount
		}
	}

	selected := b.cluster[0]
	if n >= 2 {
		candidates := rand.Perm(n)[:2]
		candidate1 := b.cluster[candidates[0]]
		candidate2 := b.cluster[candidates[1]]

		score1 := b.scoreServer(candidate1, maxInflight)
		score2 := b.scoreServer(candidate2, maxInflight)

		if score1 < score2 {
			selected = candidate1
		} else {
			selected = candidate2
		}
	}

	selected.InFlightRequestCount++
	backend := fmt.Sprintf("%s:%d", selected.Host, selected.Port)
	serverInFlight.WithLabelValues(backend).Set(float64(selected.InFlightRequestCount))
	return selected
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

func (b *BalancerState) scoreServer(srv *server.Server, maxInFlight int64) float64 {

	// so my idea is to use a simple weighted scoring
	// to compute scores for each candidate server
	var normalizedInflight float64
	if maxInFlight > 0 {
		normalizedInflight = float64(srv.InFlightRequestCount) / float64(maxInFlight)
	}

	normalizedUtilization := clamp(srv.ServerUtilization, 0, 100) / 100
	return (inFlightWeight * normalizedInflight) + (serverUtilWeight * normalizedUtilization)

}

func (b *BalancerState) releaseServer(srv *server.Server) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if srv.InFlightRequestCount > 0 {
		srv.InFlightRequestCount--
	}

	backend := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
	serverInFlight.WithLabelValues(backend).Set(float64(srv.InFlightRequestCount))
}

func (b *BalancerState) UpdateServerUtilization(srv *server.Server, utilization float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	srv.ServerUtilization = utilization

	backend := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
	serverUtilization.WithLabelValues(backend).Set(utilization)
}

func createServer(host string, port int) *server.Server {
	return &server.Server{
		Host:                 host,
		Port:                 port,
		ServerUtilization:    0.0,
		InFlightRequestCount: 0,
		RollingErrorRate:     0.0,
	}
}

func createBalancer(host string, port int) *BalancerState {
	return &BalancerState{
		host:    host,
		port:    port,
		cluster: make([]*server.Server, 0),
		current: 0,
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
				srv = createServer(entry.Service.Address, entry.Service.Port)
			}

			newCluster = append(newCluster, srv)
		}

		b.mu.Lock()
		b.cluster = newCluster
		b.mu.Unlock()

		for _, server := range b.cluster {
			log.Printf("[balancer]: registered server %s:%d", server.Host, server.Port)
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
	requestsTotal.WithLabelValues(backend).Inc()
	err := ForwardRequest(b, selected, w, req)
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
