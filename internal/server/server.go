package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"

	capi "github.com/hashicorp/consul/api"
	"github.com/mohammednumaan/flux/internal/utils"
)

/*
here Server and ServerState are different structs, they are completely independent.
Server is used to represent a server in the cluster/pool that the balancer TRACKS.
ServerState is used to represent the server utilization state that the server itself reports to the balancer.
*/
type Server struct {
	Host              string
	Port              int
	ServerUtilization float64

	// the InFlightRequestCount is local to the balancer
	// i.e number of in-flight reqs to this server from the balancer
	InFlightRequestCount int64

	// i use a percentage because a raw count by itself
	// ignores VOLUME of requests
	RollingErrorRate float32
}

type ServerState struct {
	InFlightRequestCount      int64
	MaxConfiguredRequestCount int64
}

func requestHandler(serviceName string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		log.Printf("[server]: received request from %s", req.RemoteAddr)
		fmt.Fprintf(w, "hello from %s!", serviceName)
	}
}

func healthRequestHandler(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func withUtilization(key string, state *ServerState, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&state.InFlightRequestCount, 1)
		defer atomic.AddInt64(&state.InFlightRequestCount, -1)

		inFlight := atomic.LoadInt64(&state.InFlightRequestCount)
		max := state.MaxConfiguredRequestCount

		var utilization float64
		if max > 0 {
			utilization = float64(inFlight) / float64(max) * 100
		}

		w.Header().Set(key, strconv.FormatFloat(utilization, 'f', 2, 64))
		next.ServeHTTP(w, r)
	})
}

func Start() {

	serverEnv, err := utils.GetServerEnv()
	if err != nil {
		log.Fatalf("[server]: failed to get server env: %v", err)
	}

	serverState := &ServerState{
		InFlightRequestCount:      0,
		MaxConfiguredRequestCount: serverEnv.MaxConfiguredRequestCount,
	}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/api", requestHandler(serverEnv.ServiceName))
		mux.HandleFunc("/health", healthRequestHandler)

		handler := withUtilization("X-Flux-Server-Utilization", serverState, mux)
		log.Printf("[server]: starting server on port %s", serverEnv.ServicePort)

		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", serverEnv.ServicePort), handler))
	}()

	config := capi.DefaultConfig()
	client, err := capi.NewClient(config)
	if err != nil {
		panic(err)
	}

	registration := &capi.AgentServiceRegistration{
		ID:      serverEnv.ServiceID,
		Address: serverEnv.ServiceHost,
		Name:    serverEnv.ServiceName,
		Port:    serverEnv.ServicePort,
		Check: &capi.AgentServiceCheck{
			HTTP:                           fmt.Sprintf("http://%s:%d/health", serverEnv.ServiceHost, serverEnv.ServicePort),
			Interval:                       "10s",
			Timeout:                        "5s",
			DeregisterCriticalServiceAfter: "1m",
		},
	}

	if err := client.Agent().ServiceRegister(registration); err != nil {
		log.Fatalf("[%s] failed to register with consul server agent: %v", serverEnv.ServiceID, err)
	}

	log.Printf("[%s] registered with consul server agent successfully!", serverEnv.ServiceID)
	select {}

}
