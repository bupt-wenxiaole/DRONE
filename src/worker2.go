package main

import (
	"fmt"
	"os"
	"strconv"
	"worker"
	"log"
)

func main() {
	log.Println("start")
	log.Printf("%v-----\n", os.Args[0])
	log.Printf("%v-----\n", os.Args[1])
	log.Printf("%v-----\n", os.Args[2])
	workerID, err := strconv.Atoi(os.Args[1])
	PartitionNum, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("conv fail!")
	}
	worker.RunSimWorker(workerID, PartitionNum)
	fmt.Println("stop")
}
