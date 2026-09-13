package types

type ServerEnv struct {
	ConsulHttpAddr string
	ServiceID      string
	ServiceName    string
	ServiceHost    string
	ServicePort    int
}

type BalancerEnv struct {
	ConsulHttpAddr    string
	ServiceName       string
	ServiceHost       string
	ServicePort       int
	TargetServiceName string
}
