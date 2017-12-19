package main

import (
	"bufio"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	pb "protobuf"
	"strings"
	"sync"
	//"tools"
	"tools"
	"time"
	"fmt"
	//"strconv"
)

type Master struct {
	mu      *sync.Mutex
	newCond *sync.Cond
	address string
	// signals when Register() adds to workers[]
	// number of worker
	workerNum      int
	workersAddress []string
	//master know worker's address from config.txt and worker's index
	//TCP connection Reuse : after accept resgister then dial and save the handler
	//https://github.com/grpc/grpc-go/issues/682
	grpcHandlers map[int32]*grpc.ClientConn
	//each worker's RPC address
	//TODO: split subgraphJson into worker's partition
	subGraphFiles string
	// Name of Input File subgraph json
	partitionFiles string
	// Name of Input File partition json
	shutdown     chan struct{}
	registerDone chan bool
	statistic    []int32
	//each worker's statistic data
	wg          *sync.WaitGroup
	JobDoneChan chan bool
}

func (mr *Master) Lock() {
	mr.mu.Lock()
}

func (mr *Master) Unlock() {
	mr.mu.Unlock()
}

// Register is an RPC method that is called by workers after they have started
// up to report that they are ready to receive tasks.
// Locks for multiple worker concurrently access worker list
func (mr *Master) Register(ctx context.Context, args *pb.RegisterRequest) (r *pb.RegisterResponse, err error) {
	mr.Lock()
	defer mr.Unlock()
	log.Printf("Register: worker %d\n", args.WorkerIndex)
	endpoint := mr.workersAddress[args.WorkerIndex]
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	if _, ok := mr.grpcHandlers[args.WorkerIndex]; ok {
		//TODO: if the master terminate, how about worker's register?
		log.Fatal("%d worker register more than one times", args.WorkerIndex)
	} else {
		mr.grpcHandlers[args.WorkerIndex] = conn
		if conn == nil {
			log.Println("conn is null!")
		}
	}
	if len(mr.grpcHandlers) == mr.workerNum {
		mr.registerDone <- true
	}
	log.Printf("len:%d\n", len(mr.grpcHandlers))
	log.Printf("workernum:%d\n", mr.workerNum)
	// There is no need about scheduler
	return &pb.RegisterResponse{Ok: true}, nil
}

// newMaster initializes a new Master
func newMaster() (mr *Master) {
	mr = new(Master)
	mr.shutdown = make(chan struct{})
	mr.mu = new(sync.Mutex)
	mr.newCond = sync.NewCond(mr.mu)
	mr.JobDoneChan = make(chan bool)
	mr.registerDone = make(chan bool)
	mr.wg = new(sync.WaitGroup)
	//workersAddress slice's index is worker's Index
	//read from Config text
	mr.workersAddress = make([]string, 0)
	mr.grpcHandlers = make(map[int32]*grpc.ClientConn)
	mr.statistic = make([]int32, mr.workerNum)
	return mr
}
func (mr *Master) ReadConfig() {
	f, err := os.Open(tools.ConfigPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	first := true
	for {
		line, err := rd.ReadString('\n')
		line = strings.Split(line, "\n")[0] //delete the end "\n"
		if err != nil || io.EOF == err {
			break
		}
		conf := strings.Split(line, ",")
		//TODO: this operate is for out of range
		if first {
			mr.workersAddress = append(mr.workersAddress, "0")
			mr.address = conf[1]
			log.Print(mr.address)
			first = false
		} else {
			mr.workersAddress = append(mr.workersAddress, conf[1])
			log.Print(conf[1])
		}
	}
	mr.workerNum = len(mr.workersAddress) - 1
}
func (mr *Master) wait() {
	<-mr.JobDoneChan
}
func (mr *Master) KillWorkers() {
	mr.Lock()
	defer mr.Unlock()
	for i := 1; i <= mr.workerNum; i++ {
		mr.wg.Add(1)
		log.Printf("Master: shutdown worker %d\n", i)
		handler := mr.grpcHandlers[int32(i)]
		client := pb.NewWorkerClient(handler)
		shutDownReq := &pb.ShutDownRequest{}
	        client.ShutDown(context.Background(), shutDownReq)
		//if err != nil {
		//	log.Fatal("fail to kill worker %d", i)
		//} else {
			//discuss : goland can't recognize the reply
		//		mr.statistic = append(mr.statistic, reply.IterationNum)
		//}

	}
}
func (mr *Master) StartMasterServer() {
	ln, err := net.Listen("tcp", mr.address)
	if err != nil {
		panic(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterMasterServer(grpcServer, mr)
	go func() {
		if err := grpcServer.Serve(ln); err != nil {
			panic(err)
		}
	}()

}
func (mr *Master) PEval() bool {
	for i := 1; i <= mr.workerNum; i++ {
		log.Printf("Master: start %d PEval", i)
		mr.wg.Add(1)
		go func(id int) {
			defer mr.wg.Done()
			handler, ok := mr.grpcHandlers[int32(id)]
			if !ok {
				log.Println("key error")
			}
			client := pb.NewWorkerClient(handler)
			//pevalRequest := &pb.PEvalRequest{}
			if _, err := client.PEval(context.Background(), &pb.PEvalRequest{}); err != nil {
				log.Printf("Fail to execute PEval %d\n", id)
				log.Fatal(err)
				//TODO: still something todo: Master Just terminate, how about the Worker
			}
		}(i)
	}
	mr.wg.Wait()
	return true
}

// this function is the set of all incremental superstep done
func (mr *Master) IncEvalALL() bool {
	stepCount := 0
	for {
		stepCount++
		update := false
		for i := 1; i <= mr.workerNum; i++ {
			log.Printf("Master: start the %dth PEval of worker %i", stepCount, i)
			mr.wg.Add(1)
			go func(id int) {
				defer mr.wg.Done()
				handler := mr.grpcHandlers[int32(id)]
				client := pb.NewWorkerClient(handler)
				incEvalRequest := &pb.IncEvalRequest{}
				if reply, err := client.IncEval(context.Background(), incEvalRequest); err != nil {
					log.Fatal("Fail to execute IncEval worker %v", id)
				} else {
					mr.Lock()
					//multiple goroutines access update
					update = update || reply.Update
					mr.Unlock()
				}
			}(i)
		}
		mr.wg.Wait()
		if update == false {
			break
		}
	}
	return true
}
func (mr *Master) Assemble() bool {
	for i := 1; i <= mr.workerNum; i++ {
		log.Printf("Master: start worker %d Assemble", i)
		mr.wg.Add(1)
		go func(id int) {
			defer mr.wg.Done()
			handler := mr.grpcHandlers[int32(id)]
			client := pb.NewWorkerClient(handler)
			assembleRequest := &pb.AssembleRequest{}
			if _, err := client.Assemble(context.Background(), assembleRequest); err != nil {
				log.Fatal("Fail to execute Assemble worker %v", id)
			}
		}(i)
	}
	mr.wg.Wait()
	return true
}
func (mr *Master) StopRPCServer() {

}

func RunJob(jobName string) {
	mr := newMaster()
	mr.ReadConfig()
	go mr.StartMasterServer()
	<-mr.registerDone
	log.Println("start PEval")
	start := time.Now()
	mr.PEval()
	log.Println("end PEval")
	log.Println("start IncEval")
	mr.IncEvalALL()
	log.Println("end IncEval")
	runTime := time.Since(start)
	fmt.Printf("runTime: %vs\n", runTime.Seconds() * 0.8)
	mr.Assemble()
	mr.KillWorkers()
	mr.StopRPCServer()
	//mr.wait()
	log.Printf("Job %s finishes", jobName)
}

func main() {
	jobName := "SSSP"
	//TODO:split the Json into worker's subJson
	RunJob(jobName)
}
