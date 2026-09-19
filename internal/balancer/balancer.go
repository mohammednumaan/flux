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
	selected := b.cluster[0]
	if n >= 2 {
		candidates := rand.Perm(n)[:2]
		candidate1 := b.cluster[candidates[0]]
		candidate2 := b.cluster[candidates[1]]

		if candidate1.InFlightRequestCount < candidate2.InFlightRequestCount {
			selected = candidate1
		} else {
			selected = candidate2
		}
	}

	selected.InFlightRequestCount++
	return selected
}

func (b *BalancerState) releaseServer(srv *server.Server) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if srv.InFlightRequestCount > 0 {
		srv.InFlightRequestCount--
	}
}

func (b *BalancerState) UpdateServerUtilization(srv *server.Server, utilization float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	srv.ServerUtilization = utilization
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
