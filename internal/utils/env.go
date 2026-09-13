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

func GetServerEnv() types.ServerEnv {
	consulHttpAddr := getEnv("CONSUL_HTTP_ADDR", "http://localhost:8500")
	serviceID := getEnv("SERVICE_ID", "flux-server-1")
	serviceName := getEnv("SERVICE_NAME", "flux-backend")
	serviceHost := getEnv("SERVICE_HOST", "flux-server-1")
	servicePort := getEnv("SERVICE_PORT", "8080")

	// this is something pretty cool i learnt
	// we can use := to re-assign the same variable (here servicePort)
	// as long as we introduce a new variable (here err)
	servicePortInt, err := strconv.Atoi(servicePort)
	if err != nil {
		panic(err)
	}

	return types.ServerEnv{
		ConsulHttpAddr: consulHttpAddr,
		ServiceID:      serviceID,
		ServiceName:    serviceName,
		ServiceHost:    serviceHost,
		ServicePort:    servicePortInt,
	}
}

func GetBalancerEnv() types.BalancerEnv {
	consulHttpAddr := getEnv("CONSUL_HTTP_ADDR", "http://localhost:8500")
	serviceName := getEnv("SERVICE_NAME", "flux-backend")
	serviceHost := getEnv("SERVICE_HOST", "localhost")
	servicePort := getEnv("SERVICE_PORT", "8090")
	targetServiceName := getEnv("TARGET_SERVICE_NAME", "flux-backend")

	servicePortInt, err := strconv.Atoi(servicePort)
	if err != nil {
		panic(err)
	}

	return types.BalancerEnv{
		ConsulHttpAddr:    consulHttpAddr,
		ServiceName:       serviceName,
		ServiceHost:       serviceHost,
		ServicePort:       servicePortInt,
		TargetServiceName: targetServiceName,
	}
}
