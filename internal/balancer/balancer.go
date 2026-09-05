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

func createBalancer(addr string) *BalancerState {
	return &BalancerState{
		addr:    addr,
		cluster: make([]*Server, 0),
		current: 0,
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
	}

	plan.Handler = func(idx uint64, data interface{}) {
		services, ok := data.([]*capi.ServiceEntry)
		if !ok {
			log.Println("failed to cast data to []*capi.ServiceEntry")
			return
		}

		var activeServers []string
		for _, service := range services {
			addr := service.Service.Address
			port := service.Service.Port
			activeServers = append(activeServers, fmt.Sprintf("%s:%d", addr, port))
		}

		log.Printf("[balancer]: active servers for service %s: %v", targetServiceName, activeServers)
	}

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
