package main

import (
	"worker"
	"fmt"
)

func main() {
	fmt.Println("start")
	worker.RunWorker(1)
	fmt.Println("stop")
}
