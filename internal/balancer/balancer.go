package balancer

import (
	"encoding/json"
	"github.com/mohammednumaan/flux/internal/utils"
	"log"
	"net/http"
)

/*
this is just a very simple round-robin load balancer. so far it only supports:
1. registering servers to the balancer
2. routing requests to the registered servers in a round-robin fashion
(forwarding is not implemented yet)
*/
type Server struct {
	Addr string `json:"addr"`
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

	log.Printf("forwarding request to server %s", server.Addr)
	// here would be the logic to forward
	// the request to the selected server, but for now i just log it
}

func (b *BalancerState) registerServer(w http.ResponseWriter, req *http.Request) {
	var server Server
	err := json.NewDecoder(req.Body).Decode(&server)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, s := range b.cluster {
		if s.Addr == server.Addr {
			log.Printf("server already registered %s", server.Addr)
			utils.SendRegisterHTTPResponse(w, "server already registered")
			return
		}
	}

	b.cluster = append(b.cluster, &server)
	log.Printf("server registered successfully %s", server.Addr)

	log.Printf("current cluster state: %+v", b.cluster)
	utils.SendRegisterHTTPResponse(w, "server registered successfully")
}

func Start() {
	balancer := createBalancer("http://localhost:8090")
	log.Println("[balancer]: starting balancer at port :8090")

	http.HandleFunc("/register", balancer.registerServer)
	http.HandleFunc("/api", balancer.routeRequestHandler)

	log.Fatal(http.ListenAndServe(":8090", nil))
}
