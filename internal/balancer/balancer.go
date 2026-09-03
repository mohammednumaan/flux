package balancer

import (
	"fmt"
	"log"
	"net/http"
)

/*
here are the initial assumptions i am enforcing:
1. the server cluster/pool is fixed in size.
2. each server in the cluster/pool is known beforehand.
*/

type Server struct {
	addr string
}

type BalancerState struct {
	addr    string
	cluster []Server
}

func createBalancer(addr string, cluster []Server) *BalancerState {
	return &BalancerState{
		addr:    addr,
		cluster: cluster,
	}
}

func createCluster() []Server {
	servers := make([]Server, 0)
	for i := 0; i < 3; i++ {
		server := Server{
			addr: fmt.Sprintf("http://localhost:808%d", i),
		}
		servers = append(servers, server)
	}

	return servers
}

func (b *BalancerState) routeRequestHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("received request from %s", req.RemoteAddr)
	log.Println("available servers to route request to are: ")

	for i, s := range b.cluster {
		log.Println(i, s.addr)
	}
}

func Start() {
	cluster := createCluster()
	balancer := createBalancer("http://localhost:8090", cluster)

	log.Println("[balancer]: starting balancer at port :8090")
	http.HandleFunc("/api/", balancer.routeRequestHandler)
	log.Fatal(http.ListenAndServe(":8090", nil))
}
