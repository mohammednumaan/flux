package utils

import (
	"os"
	"strconv"

	"github.com/mohammednumaan/flux/internal/types"
)

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}

	return defaultValue
}

// i don't really care about using proper alternate values for the env vars
// because once i move to k8's, the env vars will be set by the k8s deployment
func GetServerEnv() (*types.ServerEnv, error) {
	consulHttpAddr := getEnv("CONSUL_HTTP_ADDR", "http://localhost:8500")
	serviceID := getEnv("SERVICE_ID", "flux-server-1")
	serviceName := getEnv("SERVICE_NAME", "flux-backend")
	serviceHost := getEnv("SERVICE_HOST", "flux-server-1")
	servicePort := getEnv("SERVICE_PORT", "8080")
	maxConfgiuredReqCount := getEnv("MAX_CONFIGURED_REQUEST_COUNT", "100")

	servicePortInt, err := strconv.Atoi(servicePort)
	if err != nil {
		return nil, err
	}

	maxConfgiuredReqCountInt, err := strconv.ParseInt(maxConfgiuredReqCount, 10, 64)
	if err != nil {
		return nil, err
	}

	return &types.ServerEnv{
		ConsulHttpAddr:            consulHttpAddr,
		ServiceID:                 serviceID,
		ServiceName:               serviceName,
		ServiceHost:               serviceHost,
		ServicePort:               servicePortInt,
		MaxConfiguredRequestCount: maxConfgiuredReqCountInt,
	}, nil

}

func GetBalancerEnv() (*types.BalancerEnv, error) {
	consulHttpAddr := getEnv("CONSUL_HTTP_ADDR", "http://localhost:8500")
	serviceName := getEnv("SERVICE_NAME", "flux-backend")
	serviceHost := getEnv("SERVICE_HOST", "localhost")
	servicePort := getEnv("SERVICE_PORT", "8090")
	targetServiceName := getEnv("TARGET_SERVICE_NAME", "flux-backend")

	servicePortInt, err := strconv.Atoi(servicePort)
	if err != nil {
		return nil, err
	}

	return &types.BalancerEnv{
		ConsulHttpAddr:    consulHttpAddr,
		ServiceName:       serviceName,
		ServiceHost:       serviceHost,
		ServicePort:       servicePortInt,
		TargetServiceName: targetServiceName,
	}, nil
}
