package balancer

import (
	"fmt"
	"log"
	"net/http"
	"os"

	capi "github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
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
	addr    string
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

func createBalancer(addr string) *BalancerState {
	return &BalancerState{
		addr:    addr,
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
			log.Println("[balancer]: failed to cast data to []*capi.ServiceEntry")
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
				log.Printf("[balancer]: service %s is healthy", entry.Service.Service)
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
	balancer := createBalancer("http://localhost:8090")
	log.Println("[balancer]: starting balancer at port :8090")

	go func() {
		http.HandleFunc("/api", balancer.routeRequestHandler)
		log.Fatal(http.ListenAndServe(":8090", nil))
	}()

	targetServiceName := os.Getenv("TARGET_SERVICE_NAME")
	if targetServiceName == "" {
		targetServiceName = "flux-backend"
	}
	watchConfig := map[string]interface{}{
		"type":        "service",
		"service":     targetServiceName,
		"passingonly": true,
	}

	plan, err := watch.Parse(watchConfig)
	if err != nil {
		log.Fatalf("failed to create watch plan: %v", err)
		panic(err)
	}

	plan.HybridHandler = balancerWatchHandler(balancer)
	fmt.Println("[balancer]: starting consul watch for flux-backend service")

	consulServerAddr := os.Getenv("CONSUL_HTTP_ADDR")
	if consulServerAddr == "" {
		consulServerAddr = "localhost:8500"
	}

	if err := plan.Run(consulServerAddr); err != nil {
		log.Fatalf("failed to run watch plan: %v", err)
	}

	select {}

}
