package balancer

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	capi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/mohammednumaan/flux/internal/metrics"
	"github.com/mohammednumaan/flux/internal/types"
	"github.com/mohammednumaan/flux/internal/utils"
)

const (
	inFlightRequestCountWeight    = 0.35
	serverUtilizationWeight       = 0.25
	rollingErrorRateWeight        = 0.40
	maxServerUtilizationThreshold = 85.0
	maxRollingErrorRateThreshold  = 0.5
	filterMaxAttempts             = 5
)

type BalancerState struct {
	host    string
	port    int
	cluster []*types.Server
	mu      sync.Mutex
}

func (b *BalancerState) isAboveThreshold(srv *types.Server) bool {
	if srv.ServerUtilization > maxServerUtilizationThreshold {
		return true
	}

	if srv.RollingErrorRate != nil && srv.RollingErrorRate.Rate() > maxRollingErrorRateThreshold {
		return true
	}

	return false
}

func (b *BalancerState) pickOneFiltered(excludeIdx int) (*types.Server, int) {
	n := len(b.cluster)
	for i := 0; i < filterMaxAttempts; i++ {
		idx := rand.IntN(n)
		if idx == excludeIdx {
			continue
		}

		srv := b.cluster[idx]
		if !b.isAboveThreshold(srv) {
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

func (b *BalancerState) scoreServer(srv *types.Server) float64 {
	var normInFlight float64

	if srv.MaxConfiguredRequestCount > 0 {
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

func (b *BalancerState) pickServer() *types.Server {
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

	metrics.Default.RecordInFlight(backend, selected.BackendGroup, float64(selected.InFlightRequestCount))
	return selected
}

func (b *BalancerState) releaseServer(srv *types.Server) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if srv.InFlightRequestCount > 0 {
		srv.InFlightRequestCount--
	}

	backend := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
	metrics.Default.RecordInFlight(backend, srv.BackendGroup, float64(srv.InFlightRequestCount))
}

func (b *BalancerState) UpdateServerUtilization(srv *types.Server, utilization float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	srv.ServerUtilization = utilization

	backend := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
	metrics.Default.RecordUtilization(backend, srv.BackendGroup, utilization)
}

func createRollingErrorRate(timeWindow time.Duration) *types.RollingErrorRate {
	bucketSize := 1 * time.Second
	bucketCount := int(timeWindow / bucketSize)

	return &types.RollingErrorRate{
		TimeWindow: timeWindow,
		BucketSize: bucketSize,
		Buckets:    make([]types.ErrorRateBucket, bucketCount),
	}
}

func createServer(host string, port int, backendGroup string, maxRequests int64) *types.Server {
	srv := &types.Server{
		Host:                      host,
		Port:                      port,
		BackendGroup:              backendGroup,
		ServerUtilization:         0.0,
		InFlightRequestCount:      0,
		MaxConfiguredRequestCount: maxRequests,
		RollingErrorRate:          createRollingErrorRate(1 * time.Minute),
	}

	backend := fmt.Sprintf("%s:%d", host, port)
	metrics.Default.RecordRollingErrorRate(backend, backendGroup, 0)
	return srv
}

func createBalancer(host string, port int) *BalancerState {
	return &BalancerState{
		host:    host,
		port:    port,
		cluster: make([]*types.Server, 0),
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
		existing := make(map[string]*types.Server)
		for _, srv := range b.cluster {
			hostPort := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
			existing[hostPort] = srv
		}

		b.mu.Unlock()
		newCluster := make([]*types.Server, 0)
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
				backendGroup := utils.GroupFromTags(entry.Service.Tags)
				maxRequests := utils.MaxRequestsFromTags(entry.Service.Tags)

				srv = createServer(entry.Service.Address, entry.Service.Port, backendGroup, maxRequests)
			} else {
				if cap := utils.MaxRequestsFromTags(entry.Service.Tags); cap != srv.MaxConfiguredRequestCount {
					srv.MaxConfiguredRequestCount = cap
				}
			}
			newCluster = append(newCluster, srv)
		}

		b.mu.Lock()
		b.cluster = newCluster
		b.mu.Unlock()

		keep := make(map[string]struct{}, len(newCluster))
		for _, srv := range newCluster {
			keep[fmt.Sprintf("%s:%d", srv.Host, srv.Port)] = struct{}{}
		}

		for hostPort, srv := range existing {
			if _, ok := keep[hostPort]; !ok {
				metrics.Default.RecordServerRemoved(hostPort, srv.BackendGroup)
			}
		}

		for _, srv := range b.cluster {
			log.Printf("[balancer]: registered server %s:%d in group %q", srv.Host, srv.Port, srv.BackendGroup)
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
	metrics.Default.RecordRequestTotal(backend, selected.BackendGroup)

	statusCode, err := ForwardRequest(b, selected, w, req)
	failed := err != nil || statusCode >= http.StatusInternalServerError

	if selected.RollingErrorRate != nil {
		selected.RollingErrorRate.Record(failed)
		metrics.Default.RecordRollingErrorRate(backend, selected.BackendGroup, selected.RollingErrorRate.Rate())
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
			metrics.Default.Handler(),
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
