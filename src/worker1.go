package main

import (
	"worker"
	"fmt"
	"os"
	"strconv"
)

func main() {
	fmt.Println("start")
	workerID, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("conv fail!")
	}
	worker.RunWorker(workerID)
	fmt.Println("stop")
}
