package balancer

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/mohammednumaan/flux/internal/types"
)

const (
	utilizationHeader = "X-Flux-Server-Utilization"
	backendTimeout    = 10 * time.Second
)

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func ForwardRequest(b *BalancerState, s *types.Server, w http.ResponseWriter, r *http.Request) (int, error) {
	target := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", s.Host, s.Port),
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(resp *http.Response) error {
		if utilHeader := resp.Header.Get(utilizationHeader); utilHeader != "" {
			if u, err := strconv.ParseFloat(utilHeader, 64); err == nil {
				b.UpdateServerUtilization(s, u)
			}
		}
		resp.Header.Del(utilizationHeader)
		return nil
	}

	var proxyErr error
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		proxyErr = err
		w.WriteHeader(http.StatusBadGateway)
	}

	ctx, cancel := context.WithTimeout(r.Context(), backendTimeout)
	defer cancel()

	recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
	proxy.ServeHTTP(recorder, r.WithContext(ctx))

	remoteURL := target.String() + r.URL.RequestURI()
	if proxyErr != nil {
		return recorder.statusCode, proxyErr
	}

	log.Printf("[forward]: forwarded request to %s, response status: %d", remoteURL, recorder.statusCode)
	return recorder.statusCode, nil
}
