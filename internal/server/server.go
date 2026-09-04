package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	requestTypes "github.com/mohammednumaan/flux/internal/types"
)

func requestHandler(port int) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "received request from server running in port %d", port)
	}
}

func startServer(port int, mux *http.ServeMux) {
	mux.HandleFunc("/", requestHandler(port))
	addr := fmt.Sprintf(":%d", port)

	log.Printf("[server]: listening on port ::%d", port)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func Start() {
	startPort := 8080
	count := 3

	// this just spins up 3 servers and registers each of them
	// with the balancer at port 8090 via /register
	for i := 0; i < count; i++ {
		serverPort := startPort + i
		mux := http.NewServeMux()
		go startServer(serverPort, mux)

		registerRequest := requestTypes.RegisterRequest{
			Host: "localhost",
			Port: serverPort,
		}

		jsonData, err := json.Marshal(registerRequest)
		if err != nil {
			log.Fatalf("failed to marshal register request for server on port %d: %v", serverPort, err)
			panic(err)
		}

		resp, err := http.Post(
			"http://localhost:8090/register",
			"application/json",
			bytes.NewBuffer(jsonData),
		)

		defer resp.Body.Close()

		if err != nil {
			log.Fatalf("failed to register server on port %d with balancer: %v", serverPort, err)
			panic(err)
		}

		log.Printf("server on port %d registered with balancer, response status: %s", serverPort, resp.Status)
	}

}
