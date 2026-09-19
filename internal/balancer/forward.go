package balancer

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/mohammednumaan/flux/internal/server"
)

const utilizationHeader = "X-Flux-Server-Utilization"

func ForwardRequest(b *BalancerState, s *server.Server, w http.ResponseWriter, r *http.Request) error {

	remoteURL := fmt.Sprintf("http://%s:%d%s", s.Host, s.Port, r.URL.Path)
	req, err := http.NewRequest(r.Method, remoteURL, r.Body)
	if err != nil {
		return err
	}

	req.Header = r.Header.Clone()
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if utilHeader := resp.Header.Get(utilizationHeader); utilHeader != "" {
		if u, err := strconv.ParseFloat(utilHeader, 64); err == nil {
			b.UpdateServerUtilization(s, u)
		}
	}

	resp.Header.Del(utilizationHeader)

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	written, err := io.Copy(w, resp.Body)
	if err != nil {
		return err
	}
	log.Printf("[forward]: forwarded request to %s, response status: %d, bytes written: %d", remoteURL, resp.StatusCode, written)

	return nil
}
