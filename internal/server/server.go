package server

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

func homeHandler(port int) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "received request from server running in port %d", port)
	}
}

func Start() {
	startPort := 8080
	count := 3

	// this just spins up 3 servers and registers each of them
	// with the balancer at port 8090 via /register
	for i := 0; i < count; i++ {
		serverPort := startPort + i
		mux := http.NewServeMux()
		mux.HandleFunc("/", homeHandler(serverPort))

		go func(port int, handler http.Handler) {
			addr := fmt.Sprintf(":%d", port)
			log.Printf("listening on port ::%d", port)
			log.Fatal(http.ListenAndServe(addr, handler))
		}(serverPort, mux)

		resp, err := http.Post(
			"http://localhost:8090/register",
			"application/json",
			strings.NewReader(fmt.Sprintf(`{"addr": "http://localhost:%d"}`, serverPort)),
		)

		if err != nil {
			log.Fatalf("failed to register server on port %d with balancer: %v", serverPort, err)
			panic(err)
		}

		defer resp.Body.Close()
		log.Prinf("server on port %d registered successfully with balancer", serverPort)
	}

}
