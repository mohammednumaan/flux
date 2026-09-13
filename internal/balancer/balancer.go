package balancer

import (
	"fmt"
	"log"
	"net/http"

	capi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/mohammednumaan/flux/internal/utils"
)

/*
this is just a very simple round-robin load balancer. so far it only supports:
1. registering servers to the balancer
2. routing requests to the registered servers in a round-robin fashion
(forwarding is not implemented yet)
*/
type Server struct {
	Host              string
	Port              int
	ServerUtilization float32

	// the InFlightRequestCount is local to the balancer
	// i.e number of in-flight reqs to this server from the balancer
	InFlightRequestCount int

	// i use a percentage because a raw count by itself
	// ignores VOLUME of requests
	RollingErrorRate float32
}

type BalancerState struct {
	host    string
	port    int
	cluster []*Server
	current int
}

func createServer(host string, port int) *Server {
	return &Server{
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
		cluster: make([]*Server, 0),
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

		newCluster := make([]*Server, 0)
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
	log.Printf("received request from %s", req.RemoteAddr)
	serverIdx := b.current % len(b.cluster)

	server := b.cluster[serverIdx]
	b.current++

	serverAddr := fmt.Sprintf("%s:%d", server.Host, server.Port)
	log.Printf("forwarding request to server %s", serverAddr)
	// here would be the logic to forward
	// the request to the selected server, but for now i just log it
}

func Start() {
	balancerEnv := utils.GetBalancerEnv()
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
