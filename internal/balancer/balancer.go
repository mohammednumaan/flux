package balancer

import (
	"fmt"
	"log"
	"math"
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
	maxServerUtilizationThreshold = 75.0
	maxRollingErrorRateThreshold  = 0.5

	// fallback capacity used for scoring when a server does not "expose" a
	// valid flux-max-requests tag, so it is not treated as having infinite
	// capacity to handle requests
	defaultMaxConfiguredRequestCount = 100
)

type BalancerState struct {
	host        string
	port        int
	cluster     []*types.Server
	inProbation map[string]bool
	mu          sync.Mutex
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

func (b *BalancerState) probationLimited(srv *types.Server) bool {
	hostPort := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
	return b.inProbation[hostPort] && srv.InFlightRequestCount >= 1
}

func (b *BalancerState) pickOneFiltered(excludeIdx int) (*types.Server, int) {
	eligible := make([]int, 0, len(b.cluster))
	fallback := make([]int, 0, len(b.cluster))

	for idx, srv := range b.cluster {
		if idx == excludeIdx || b.probationLimited(srv) {
			continue
		}

		fallback = append(fallback, idx)
		if !b.isAboveThreshold(srv) {
			eligible = append(eligible, idx)
		}
	}

	// prefer servers under their health thresholds, but fall back to any
	// server that is not already occupied by a probation request.
	candidates := eligible
	if len(candidates) == 0 {
		candidates = fallback
	}
	if len(candidates) == 0 {
		return nil, -1
	}

	idx := candidates[rand.IntN(len(candidates))]
	return b.cluster[idx], idx
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

	maxConfigured := srv.MaxConfiguredRequestCount
	if maxConfigured <= 0 {
		maxConfigured = defaultMaxConfiguredRequestCount
	}
	normInFlight = float64(srv.InFlightRequestCount) / float64(maxConfigured)

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

	selected, idx := b.pickOneFiltered(-1)
	if selected == nil {
		return nil
	}

	if n >= 2 {
		candidate2, _ := b.pickOneFiltered(idx)
		if candidate2 != nil {
			score1 := b.scoreServer(selected)
			score2 := b.scoreServer(candidate2)

			if score1 >= score2 {
				selected = candidate2
			}
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
	if math.IsNaN(utilization) || math.IsInf(utilization, 0) || utilization < 0 {
		log.Printf("[balancer]: ignoring invalid utilization %v reported by server %s:%d", utilization, srv.Host, srv.Port)
		return
	}

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
		host:        host,
		port:        port,
		cluster:     make([]*types.Server, 0),
		inProbation: make(map[string]bool),
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
			log.Printf("[balancer]: failed to cast data to []*capi.ServiceEntry")
			return
		}

		b.mu.Lock()
		existing := make(map[string]*types.Server)
		for _, srv := range b.cluster {
			hostPort := fmt.Sprintf("%s:%d", srv.Host, srv.Port)
			existing[hostPort] = srv
		}

		newCluster := make([]*types.Server, 0, len(services))
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
				b.inProbation[hostPort] = true
				metrics.Default.RecordProbation(hostPort, backendGroup, true)
			} else {
				if cap := utils.MaxRequestsFromTags(entry.Service.Tags); cap > 0 && cap != srv.MaxConfiguredRequestCount {
					srv.MaxConfiguredRequestCount = cap
				}
			}
			newCluster = append(newCluster, srv)
		}

		b.cluster = newCluster

		keep := make(map[string]struct{}, len(newCluster))
		for _, srv := range newCluster {
			keep[fmt.Sprintf("%s:%d", srv.Host, srv.Port)] = struct{}{}
		}

		for hostPort, srv := range existing {
			if _, ok := keep[hostPort]; !ok {
				metrics.Default.RecordServerRemoved(hostPort, srv.BackendGroup)
				delete(b.inProbation, hostPort)
			}
		}

		logCluster := append([]*types.Server(nil), b.cluster...)
		b.mu.Unlock()

		for _, srv := range logCluster {
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
		// ForwardRequest's proxy error handler already wrote a 502 to the client.
		log.Printf("[balancer]: failed to forward request to server %s:%d: %v", selected.Host, selected.Port, err)
		return
	}

	// if the server was in probation and we successfully received a response
	// from it, we can remove it from probation and start sending traffic to it
	if !failed {
		b.mu.Lock()
		if b.inProbation[backend] {
			delete(b.inProbation, backend)
			metrics.Default.RecordProbation(backend, selected.BackendGroup, false)
		}
		b.mu.Unlock()
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
