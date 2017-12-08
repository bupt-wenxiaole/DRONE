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
	//TODO: TCP connection reuse
	//first method: when first call save the handler
	//second method: after accept resgister then dial and save the handler
	//https://github.com/grpc/grpc-go/issues/682
	grpcHandlers []*grpc.ClientConn
	//each worker's RPC address
	subGraphFiles []string
	// Name of Input File subgraph json
	partitionFiles []string
	// Name of Input File partition json
	shutdown chan struct{}
	registerDone chan bool
	statistic []int
	//each worker's statistic data
	PEvalDone chan bool
	IncreDone chan bool
	AssembleDone chan bool
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
func (mr *Master) Register(ctx context.Context， args *RegisterRequest) （r *RegisterResponse， error） {
	mr.lock()
	defer mr.unlock()
	log.Printf("Register: worker %s\n", args.Worker)
	endpoint := args.workerIP + ':' + args.workerPort
	mr.workers = append(mr.workers, endpoint)
    conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
    if err != nil {
        panic(err)
    }
    mr.grpcHandlers = append(mr.grpcHandlers, conn)
	if len(worker)  == mr.workerNum {
		mr.registerDone <- true
	}
	// There is no need about scheduler
	return true, nil
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
	mr.PEvalDone = make(chan bool)
	mr.IncEvalDone = make(chan bool)
	mr.AssembleDone = make(chan bool)
	mr.workers = make([]string, mr.workerNum)
	mr.grpcHandlers = make([]*grpc.ClientConn, mr.workerNum)
	mr.statistic = make([]int, mr.workerNum)
	return
}

func (mr *Master) wait() {
	<-mr.doneChannel
}
func (mr *Master) killWorkers() {
	mr.lock()
	defer mr.unlock()
	for i, w := range mr.workers {
		log.Printf("Master: shutdown worker %s\n", w)
		//reply := &pb.ShutDownResponse{}
		handler := mr.grpcHandlers[i]
		client := pb.Newwoerker_servcieClient(handler)
		if reply, err := client.ShutDown(context.Background(), &struct{}) , err != nil {
			log.Fatal("fail to kill worker %s", w)
		}

			mr.statistic = append(mr.statistic, reply.iterationNum)
		}

	}
}
func (mr *Master) startMasterServer(port string) {
	ln, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterMaster(grpcServer, mr)
	go func() {
		if err := grpcServer.Serve(ln); err != nil {
			panic(err)
		}
	}()

}
func (mr *Master) PEval(subGraphJson []string, partitionJson []string) ok bool{
	cnt := 0
	for i, w := range mr.workers {
		log.Printf("Master: start %s PEval", w)
		go func() {
			handler := mr.grpcHandlers[i]
			client := pb.Newwoerker_servcieClient(handler)
			var pevalrequset *PEvalRequest = &PEvalRequest{subGraphJson: subGraphJson[i], partitionJson: partitionJson[i]}
			if reply, err := client.PEval(context.Background(), pevalrequset), err != nil {
				log.Fatal("Fail to execute PEval %s", w)
			}
			else {
				mr.lock()
				cnt++
				if cnt == mr.workerNum{
					mr.PEvalDone <- true

				}
				mr.unlock()
			}
		}()
	}
	<- mr.PEvalDone
	return true

}
// this function is the set of all incremental superstep done
func (mr *Master) IncEvalALL() ok bool {
	stepCount := 0
	for {
		stepCount++
		cnt := 0
		update := false
		for i, w := range mr.workers {
			log.Printf("Master: start the %dth PEval of %s", stepCount, w)
			go func() {
				handler := mr.grpcHandlers[i]
				client := pb.Newwoerker_servcieClient(handler)
				if reply, err := client.IncEval(context.Background(), &struct{}), err != nil {
					log.Fatal("Fail to execute IncEval %s", w)
				}
				else {
					mr.lock()
					cnt ++
					update = update || reply.update
					if cnt == mr.workerNum {
						mr.IncEvalDone <- true
					}
					mr.unlock()
				}
			}
		}
		<- mr.IncEvalDone
		if update == false {
			break
		}
	}
	return true
}
func (mr *Master) Assemble() ok bool{
	cnt := 0
	for i, w := range mr.workers {
		log.Printf("Master: start %s Assemble", w)
		go func() {
			handler := mr.grpcHandlers[i]
			client := pb.Newwoerker_servcieClient(handler)
			if reply, err := client.Assemble(context.Background(), &struct{}), err != nil {
				log.Fatal("Fail to execute Assemble %s", w)
			}
			else {
				mr.lock()
				cnt++
				if cnt == mr.workerNum{
					mr.AssembleDone <- true

				}
				mr.unlock()
			}
		}()
	}
	<- mr.AssembleDone
	return true
}

func RunJob(jobName string, subGraphJson []string, partitionJson []string, nWorker int, master string) {
	mr = newMaster(master)
	mr.startMasterServer()
	mr.PEval(subGraphJson, partitionJson)
	mr.IncEval()
	mr.Assemble()
	mr.killWorkers()
	mr.stopRPCServer()
	mr.wait()
	log.Printf("Job finishes")
}


