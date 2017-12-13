package main

import (
	"worker"
	"fmt"
)

func main() {
	fmt.Println("start")
	worker.RunWorker(1)
	fmt.Println("wait")
	worker.RunWorker(2)
	fmt.Println("stop")
}
