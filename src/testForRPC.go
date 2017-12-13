package main

import (
	"worker"
)

func main() {
	go worker.RunWorker(1)
	go worker.RunWorker(2)
}
