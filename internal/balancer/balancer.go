package balancer

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"

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
}

/*
this is just a very simple round-robin load balancer. so far it only supports:
1. registering servers to the balancer
2. routing requests to the registered servers in a round-robin fashion
(forwarding is not implemented yet)
*/

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

		newCluster := make([]*server.Server, 0)
		for _, entry := range services {
			status := "passing"
			for _, check := range entry.Checks {
				if check.Status != capi.HealthPassing {
					status = "unhealthy"
					break
				}
			}

			if status == "passing" {
				log.Printf("[balancer]: service with address %s:%d is healthy", entry.Service.Address, entry.Service.Port)
				server := createServer(entry.Service.Address, entry.Service.Port)
				newCluster = append(newCluster, server)
			} else {
				log.Printf("[balancer]: service %s is unhealthy", entry.Service.Service)

			}
		}

		b.cluster = newCluster
		for _, server := range b.cluster {
			log.Printf("[balancer]: registered server %s:%d", server.Host, server.Port)
		}

	}
}

func (b *BalancerState) routeRequestHandler(w http.ResponseWriter, req *http.Request) {
	// log.Printf("[balancer]: received request %s from %s", req.URL.Path, req.RemoteAddr)

	if len(b.cluster) == 0 {
		// log.Printf("[balancer]: no servers available to handle request %s from %s", req.URL.Path, req.RemoteAddr)
		http.Error(w, "No servers available", http.StatusServiceUnavailable)
		return
	}

	// choice of 2 algorithm. here, i select two servers at random
	// and pick one using various factors such as utilization, in-flight requests
	// and rolling error rate.
	n := len(b.cluster)
	// for _, server := range b.cluster {
	// 	log.Printf("[balancer]: server %s:%d has %d in-flight requests", server.Host, server.Port, server.InFlightRequestCount)
	// }

	var serverIdx int
	if n >= 2 {
		// this generates 2 random numbers which are unique
		// from 0 to n, where n is the number of servers in the cluster
		candidates := rand.Perm(n)[:2]
		candidate1 := b.cluster[candidates[0]]
		candidate2 := b.cluster[candidates[1]]

		// i am only focusing on in-flight requests for now
		if candidate1.InFlightRequestCount < candidate2.InFlightRequestCount {
			serverIdx = candidates[0]
		} else {
			serverIdx = candidates[1]
		}

	}

	selectedServer := b.cluster[serverIdx]
	err := ForwardRequest(selectedServer, w, req)
	if err != nil {
		// log.Printf("[balancer]: failed to forward request %s from %s to server %s:%d: %v", req.URL.Path, req.RemoteAddr, selectedServer.Host, selectedServer.Port, err)
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
