package main

import (
	"fmt"
	"os"
	"strconv"
	"worker"
)

func main() {
	fmt.Println("start")
	fmt.Printf("%v-----\n", os.Args[0])
	fmt.Printf("%v-----\n", os.Args[1])
	fmt.Printf("%v-----\n", os.Args[2])
	workerID, err := strconv.Atoi(os.Args[1])
	PartitionNum, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("conv fail!")
	}
	worker.RunCCWorker(workerID, PartitionNum)
	fmt.Println("stop")
}