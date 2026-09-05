package main

import (
	"github.com/mohammednumaan/flux/internal/server"
)

func main() {
	go server.Start()
	select {}
}
