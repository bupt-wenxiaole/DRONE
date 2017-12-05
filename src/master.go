package grape

import (
	"log"
	"net"
	"time"
	pb "protobuf"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"sync"
	"mapreduce"
)

type Master struct {
	mutex *sync.Mutex
	newCond *syc.Cond
	// signals when Register() adds to workers[]
	// number of worker
	workerNum  int
	workers []string
	//each worker's RPC address
	subGraphFiles []string
	// Name of Input File subgraph json
	partitionFiles []string
	// Name of Input File partition json
	shutdown chan struct{}
	registerDone chan bool
	statistic []int
	//each worker's statistic data
}

func (mr *Master) lock() {
	mr.mutex.Lock()
}

func (mr *Master) unlock() {
	mr.mutex.Unlock()
}


// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
// Locks for multiple worker concurrently access worker list
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.lock()
	defer mr.unlock()
	log.Printf("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)
	if len(worker)  == mr.workerNum {
		mr.registerDone <- true
	}
	// There is no need about scheduler
	return nil
}

// newMaster initializes a new Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.mutex = new(sync.Mutex)
	mr.newCond = sync.NewCond(mr.mutex)
	mr.doneChannel = make(chan bool)
	mr.registerDone = make(chan bool)
	return
}
func (mr *Master) wait() {
	<-mr.doneChannel
}
func (mr *Master) killWorkers() {
	mr.lock()
	defer mr.unlock()
	for _, w := range mr.workers {
		log.Printf("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := Grpc.call(worker. shutdown)
		if ok == false {
			log.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			mr.statistic = append(mr.statistic, reply.Niter)
		}

	}
}

func RunMaster(jobName string, subGraphJson []string, partitionJson []string, nWorker int, master string) {
	mr = newMaster(master)
	mr.startRPCServer()
	mr.PEval()
	mr.Incval()
	mr.Assemble()
	mr.killWorkers()
	mr.stopRPCServer()
	mr.wait()
	log.Printf("Job finishes")
}


