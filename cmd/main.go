package main

import (
	"github.com/mohammednumaan/flux/internal/balancer"
	"github.com/mohammednumaan/flux/internal/server"
)

func main() {
	go balancer.Start()
	go server.Start()
	select {}
}
