package server

import (
	"fmt"
	"log"
	"net/http"

	capi "github.com/hashicorp/consul/api"
	"github.com/mohammednumaan/flux/internal/utils"
)

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

func Start() {

	serverEnv, err := utils.GetServerEnv()
	if err != nil {
		log.Fatalf("[server]: failed to get server env: %v", err)
	}

	go func() {
		log.Printf("[server]: starting server on port %s", serverEnv.ServicePort)

		http.HandleFunc("/api", requestHandler(serverEnv.ServiceName))
		http.HandleFunc("/health", healthRequestHandler)

		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", serverEnv.ServicePort), nil))
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
