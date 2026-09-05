package server

import (
	"fmt"
	"log"
	"net/http"
	"os"

	capi "github.com/hashicorp/consul/api"
)

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

func requestHandler(port int) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "received request from server running in port %d", port)
	}
}

func healthRequestHandler(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func Start() {

	go func() {
		log.Println("[server]: starting server on port 8080")

		http.HandleFunc("/", requestHandler(8080))
		http.HandleFunc("/health", healthRequestHandler)

		err := http.ListenAndServe(":8080", nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	serviceID := getEnv("SERVICE_ID", "flux-server-1")
	serviceName := getEnv("SERVICE_NAME", "flux-backend")
	serviceHost := getEnv("SERVICE_HOST", "flux-server-1")

	config := capi.DefaultConfig()
	client, err := capi.NewClient(config)
	if err != nil {
		panic(err)
	}

	registration := &capi.AgentServiceRegistration{
		ID:   serviceID,
		Name: serviceName,
		Port: 8080,
		Check: &capi.AgentServiceCheck{
			HTTP:                           fmt.Sprintf("http://%s:8080/health", serviceHost),
			Interval:                       "10s",
			Timeout:                        "5s",
			DeregisterCriticalServiceAfter: "1m",
		},
	}

	if err := client.Agent().ServiceRegister(registration); err != nil {
		log.Fatalf("[%s] failed to register: %v", serviceID, err)
		panic(err)
	}

	log.Printf("[%s] registered with consul successfully!", serviceID)
	select {}

}
