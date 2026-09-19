package types

type ServerEnv struct {
	ConsulHttpAddr            string
	ServiceID                 string
	ServiceName               string
	ServiceHost               string
	ServicePort               int
	MaxConfiguredRequestCount int64
}

type BalancerEnv struct {
	ConsulHttpAddr    string
	ServiceName       string
	ServiceHost       string
	ServicePort       int
	TargetServiceName string
}
