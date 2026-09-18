package balancer

import (
	"fmt"
	"io"
	// "log"
	"net/http"
	"sync/atomic"

	"github.com/mohammednumaan/flux/internal/server"
)

func ForwardRequest(s *server.Server, w http.ResponseWriter, r *http.Request) error {

	remoteURL := fmt.Sprintf("http://%s:%d%s", s.Host, s.Port, r.URL.Path)
	req, err := http.NewRequest(r.Method, remoteURL, r.Body)
	if err != nil {
		return err
	}

	req.Header = r.Header.Clone()
	atomic.AddInt64(&s.InFlightRequestCount, 1)
	defer atomic.AddInt64(&s.InFlightRequestCount, -1)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	_, writeErr := io.Copy(w, resp.Body)
	if writeErr != nil {
		return writeErr
	}
	// log.Printf("[forward]: forwarded request to %s, response status: %d, bytes written: %d", remoteURL, resp.StatusCode, written)

	return nil
}
