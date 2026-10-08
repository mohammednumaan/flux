package utils

import (
	"log"
	"strconv"
	"strings"
)

const (
	BackendGroupTag       = "flux-backend-group="
	BackendMaxRequestsTag = "flux-max-requests="
)

func GroupFromTags(tags []string) string {
	for _, tag := range tags {
		after, ok := strings.CutPrefix(tag, BackendGroupTag)
		if ok {
			return after
		}
	}

	return "unknown"
}

func MaxRequestsFromTags(tags []string) int64 {
	for _, tag := range tags {
		after, ok := strings.CutPrefix(tag, BackendMaxRequestsTag)
		if ok {
			maxRequests, err := strconv.ParseInt(after, 10, 64)
			if err != nil {
				log.Printf("[balancer]: failed to parse max requests from tag %q: %v", tag, err)
				return 0
			}
			return maxRequests
		}
	}

	return 0
}
