package utils

import (
	"io"
	"log"
	"net/http"
)

func ForwardRequest(remoteURL string, w http.ResponseWriter, r *http.Request) error {

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
