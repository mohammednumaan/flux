package balancer

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	requestTypes "github.com/mohammednumaan/flux/internal/types"
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

func (b *BalancerState) registerServer(w http.ResponseWriter, req *http.Request) {
	var registerRequest requestTypes.RegisterRequest
	err := json.NewDecoder(req.Body).Decode(&registerRequest)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	newServerAddr := fmt.Sprintf("%s:%d", registerRequest.Host, registerRequest.Port)
	for _, s := range b.cluster {
		currentServerAddr := fmt.Sprintf("%s:%d", s.Host, s.Port)
		if currentServerAddr == newServerAddr {
			utils.SendRegisterHTTPResponse(w, "server already registered")
			return
		}
	}

	newServer := Server{
		Host:                 registerRequest.Host,
		Port:                 registerRequest.Port,
		ServerUtilization:    0.0,
		InFlightRequestCount: 0,
		RollingErrorRate:     0.0,
	}

	b.cluster = append(b.cluster, &newServer)
	utils.SendRegisterHTTPResponse(w, "server registered successfully")
}

func Start() {
	balancer := createBalancer("http://localhost:8090")
	log.Println("[balancer]: starting balancer at port :8090")

	http.HandleFunc("/register", balancer.registerServer)
	http.HandleFunc("/api", balancer.routeRequestHandler)

	log.Fatal(http.ListenAndServe(":8090", nil))
}
