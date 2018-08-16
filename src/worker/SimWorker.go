package worker

import (
	"algorithm"
	"bufio"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"graph"
	"io"
	"log"
	"net"
	"os"
	pb "protobuf"
	"strconv"
	"strings"
	"sync"
	"time"
	"tools"
	"Set"
	"sort"
)

type SimWorker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	workerNum int

	g       graph.Graph
	grpcHandlers map[int]*grpc.ClientConn
	pattern graph.Graph
	sim     map[graph.ID]Set.Set

	//edge_count int64

	calMessages []*algorithm.SimPair
	exchangeMessages []*algorithm.SimPair

	iterationNum int64
	stopChannel  chan bool

	allNodeUnionFO Set.Set
}

func (w *SimWorker) Lock() {
	w.mutex.Lock()
}

func (w *SimWorker) UnLock() {
	w.mutex.Unlock()
}

func (w *SimWorker) ShutDown(ctx context.Context, args *pb.ShutDownRequest) (*pb.ShutDownResponse, error) {
	log.Println("receive shutDown request")
	w.Lock()
	defer w.Lock()

	for i, handle := range w.grpcHandlers {
		if i == 0 || i == w.selfId {
			continue
		}
		handle.Close()
	}
	w.stopChannel <- true
	log.Println("shutdown ok")
	return &pb.ShutDownResponse{IterationNum: int32(w.iterationNum)}, nil
}


// rpc send has max size limit, so we spilt our transfer into many small block
func Peer2PeerSimSend(client pb.WorkerClient, message []*pb.SimMessageStruct, partitionId int, srcId int)  {
	for len(message) > tools.RPCSendSize {
		slice := message[0:tools.RPCSendSize]
		message = message[tools.RPCSendSize:]
		_, err := client.SimSend(context.Background(), &pb.SimMessageRequest{Pair: slice})
		if err != nil {
			log.Printf("%v send to %v error", srcId, partitionId)
			log.Fatal(err)
		}
	}
	if len(message) != 0 {
		_, err := client.SimSend(context.Background(), &pb.SimMessageRequest{Pair: message})
		if err != nil {
			log.Printf("%v send to %v error", srcId, partitionId)
			log.Fatal(err)
		}
	}
}

func (w *SimWorker) peVal(args *pb.PEvalRequest, id int) {
	var fullSendStart time.Time
	var fullSendDuration float64
	SlicePeerSend := make([]*pb.WorkerCommunicationSize, 0)
	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum := algorithm.GraphSim_PEVal(w.g, w.pattern, w.sim, w.allNodeUnionFO, w.preSet, w.postSet)
	w.allNodeUnionFO = nil
	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place. contains nothing, client end should ignore it

		masterHandle := w.grpcHandlers[0]
		Client := pb.NewMasterClient(masterHandle)

		finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
			AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
			CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: 0,
			PairNum: SlicePeerSendNull, WorkerID: int32(id), MessageToSend: isMessageToSend}

		Client.SuperStepFinish(context.Background(), finishRequest)
		return
	} else {
		fullSendStart = time.Now()
		var wg sync.WaitGroup
		messageLen := len(messages)
		batch := (messageLen + tools.ConnPoolSize - 1) / tools.ConnPoolSize

		indexBuffer := make([]int, 0)
		for partitionId := range messages {
			indexBuffer = append(indexBuffer, partitionId)
		}
		sort.Ints(indexBuffer)
		start := 0
		for i := 1; i < len(indexBuffer); i++ {
			if indexBuffer[i] > id {
				start = i
				break
			}
		}
		indexBuffer = append(indexBuffer[start:], indexBuffer[:start]...)

		for i := 1; i <= batch; i++ {
			for j := (i - 1) * tools.ConnPoolSize; j < i * tools.ConnPoolSize && j < len(indexBuffer); j++ {
				partitionId := indexBuffer[j]
				message := messages[partitionId]
				//delete(messages, partitionId)
				wg.Add(1)
				eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{WorkerID:int32(partitionId + 1), CommunicationSize:int32(len(message))}
				SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)

				go func(partitionId int, message []*algorithm.SimPair) {
					defer wg.Done()
					workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
					if err != nil {
						log.Fatal(err)
					}
					defer workerHandle.Close()

					client := pb.NewWorkerClient(workerHandle)
					encodeMessage := make([]*pb.SimMessageStruct, 0)
					for _, msg := range message {
						encodeMessage = append(encodeMessage, &pb.SimMessageStruct{PatternId: msg.PatternNode.IntVal(), DataId: msg.DataNode.IntVal()})
					}
					Peer2PeerSimSend(client, encodeMessage, partitionId + 1, id)
				}(partitionId, message)

			}
			wg.Wait()
		}
	}
	fullSendDuration = time.Since(fullSendStart).Seconds()

	masterHandle := w.grpcHandlers[0]
	Client := pb.NewMasterClient(masterHandle)

	finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
		AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

	Client.SuperStepFinish(context.Background(), finishRequest)
	return
}

func (w *SimWorker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
	go w.peVal(args, w.selfId)
	return &pb.PEvalResponse{Ok:true}, nil
}

func (w *SimWorker) incEval(args *pb.IncEvalRequest, id int) {
	w.iterationNum++
	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, aggregateTime,
	aggregatorOriSize, aggregatorReducedSize := algorithm.GraphSim_IncEval(w.g, w.pattern, w.sim, w.message, w.preSet, w.postSet)
	w.message = make([]*algorithm.SimPair, 0)
	var fullSendStart time.Time
	var fullSendDuration float64
	SlicePeerSend := make([]*pb.WorkerCommunicationSize, 0)
	if !isMessageToSend {

		masterHandle := w.grpcHandlers[0]
		Client := pb.NewMasterClient(masterHandle)

		finishRequest := &pb.FinishRequest{AggregatorOriSize: aggregatorOriSize,
			AggregatorSeconds: aggregateTime, AggregatorReducedSize: aggregatorReducedSize, IterationSeconds: iterationTime,
			CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: 0,
			PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

		Client.SuperStepFinish(context.Background(), finishRequest)
		return
	} else {
		fullSendStart = time.Now()
		var wg sync.WaitGroup

		messageLen := len(messages)
		batch := (messageLen + tools.ConnPoolSize - 1) / tools.ConnPoolSize

		indexBuffer := make([]int, 0)
		for partitionId := range messages {
			indexBuffer = append(indexBuffer, partitionId)
		}
		sort.Ints(indexBuffer)
		start := 0
		for i := 1; i < len(indexBuffer); i++ {
			if indexBuffer[i] > id {
				start = i
				break
			}
		}
		indexBuffer = append(indexBuffer[start:], indexBuffer[:start]...)

		for i := 1; i <= batch; i++ {
			for j := (i - 1) * tools.ConnPoolSize; j < i * tools.ConnPoolSize && j < len(indexBuffer); j++ {
				wg.Add(1)
				partitionId := indexBuffer[j]
				message := messages[partitionId]
				eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{WorkerID:int32(partitionId + 1), CommunicationSize:int32(len(message))}
				SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)
				go func(partitionId int, message []*algorithm.SimPair) {
					defer wg.Done()
					workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
					if err != nil {
						log.Fatal(err)
					}
					defer workerHandle.Close()

					client := pb.NewWorkerClient(workerHandle)
					encodeMessage := make([]*pb.SimMessageStruct, 0)

					for _, msg := range message {
						encodeMessage = append(encodeMessage, &pb.SimMessageStruct{PatternId: msg.PatternNode.IntVal(), DataId: msg.DataNode.IntVal()})
					}
					Peer2PeerSimSend(client, encodeMessage, partitionId + 1, id)
				}(partitionId, message)

			}
			wg.Wait()
		}
	}
	fullSendDuration = time.Since(fullSendStart).Seconds()

	masterHandle := w.grpcHandlers[0]
	Client := pb.NewMasterClient(masterHandle)

	finishRequest := &pb.FinishRequest{AggregatorOriSize: aggregatorOriSize,
		AggregatorSeconds: aggregateTime, AggregatorReducedSize: aggregatorReducedSize, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

	Client.SuperStepFinish(context.Background(), finishRequest)
	return
}

func (w *SimWorker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	log.Printf("start IncEval, updated message size:%v\n", len(w.message))
	go w.incEval(args, w.selfId)
	return &pb.IncEvalResponse{Update:true}, nil
}

func (w *SimWorker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	log.Println("assemble!")
	innerNodes := w.g.GetNodes()

	var f *os.File
	if tools.WorkerOnSC {
		f, _ = os.Create(tools.ResultPath + "simresult_" + strconv.Itoa(w.selfId-1))
	} else {
		f, _ = os.Create(tools.ResultPath + "/result_" + strconv.Itoa(w.selfId-1))
	}
	defer f.Close()
	writer := bufio.NewWriter(f)

	//result := make([]string, 0)
	size := 0
	for u, simSets := range w.sim {
		for v := range simSets {
			if _, ok := innerNodes[v]; ok {
				size++
				if size < 100 {
					writer.WriteString(u.String() + "\t" + v.String() + "\n")
				}
			}
		}
	}
	writer.Flush()

	//return &pb.AssembleResponse{Ok: ok}, nil
	return &pb.AssembleResponse{Ok: true}, nil
}

func (w *SimWorker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
	return nil, nil
}

func (w *SimWorker) PRSend(ctx context.Context, args *pb.PRMessageRequest) (*pb.PRMessageResponse, error) {
	return nil, nil
}

func (w *SimWorker) SimSend(ctx context.Context, args *pb.SimMessageRequest) (*pb.SimMessageResponse, error) {
	log.Println("sim send receive")
	message := make([]*algorithm.SimPair, 0)
	for _, messagePair := range args.Pair {
		message = append(message, &algorithm.SimPair{DataNode: graph.ID(messagePair.DataId), PatternNode: graph.ID(messagePair.PatternId)})
	}

	w.Lock()
	w.message = append(w.message, message...)
	w.UnLock()

	return &pb.SimMessageResponse{}, nil
}

func newSimWorker(id, partitionNum int) *SimWorker {
	w := new(SimWorker)
	w.mutex = new(sync.Mutex)
	w.selfId = id
	w.peers = make([]string, 0)
	w.iterationNum = 0
	w.stopChannel = make(chan bool)
	w.message = make([]*algorithm.SimPair, 0)
	w.sim = make(map[graph.ID]Set.Set)
	w.grpcHandlers = make(map[int]*grpc.ClientConn)

	// read config file get ip:port config
	// in config file, every line in this format: id,ip:port\n
	// while id means the id of this worker, and 0 means master
	// the id of first line must be 0 (so the first ip:port is master)
	f, err := os.Open(tools.ConfigPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		line = strings.Split(line, "\n")[0] //delete the end "\n"
		if err != nil || io.EOF == err {
			break
		}

		conf := strings.Split(line, ",")
		w.peers = append(w.peers, conf[1])
	}

	start := time.Now()
	w.workerNum = partitionNum

	var graphIO, master, mirror *os.File

	if tools.WorkerOnSC {
		graphIO, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/G." + strconv.Itoa(w.selfId-1))
	} else {
		graphIO, _ = os.Open(tools.NFSPath + "G." + strconv.Itoa(w.selfId-1))
	}
	defer graphIO.Close()

	if graphIO == nil {
		fmt.Println("graph is nil")
	}
	if tools.WorkerOnSC {
		master, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/Master." + strconv.Itoa(w.selfId-1))
		mirror, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/Mirror." + strconv.Itoa(w.selfId-1))
	} else {
		master, _ = os.Open(tools.NFSPath + "Master." + strconv.Itoa(w.selfId-1))
		mirror, _ = os.Open(tools.NFSPath + "Mirror." + strconv.Itoa(w.selfId-1))
	}
	defer master.Close()
	defer mirror.Close()

	w.g, err = graph.NewGraphFromTXT(graphIO, master, mirror)
	if err != nil {
		log.Fatal(err)
	}

	patternFile, err := os.Open(tools.PatternPath)
	if err != nil {
		log.Fatal("pattern path error")
	}
	defer patternFile.Close()
	w.pattern, _ = graph.NewPatternGraph(patternFile)

	loadTime := time.Since(start)
	log.Printf("loadGraph Time: %v\n", loadTime)
	fmt.Printf("loadGraph Time: %v\n", loadTime)

	if w.g == nil {
		log.Println("can't load graph")
	}

	return w
}

func RunSimWorker(id, partitionNum int) {
	w := newSimWorker(id, partitionNum)

	log.Println(w.selfId)
	log.Println(w.peers[w.selfId])
	ln, err := net.Listen("tcp", ":"+strings.Split(w.peers[w.selfId], ":")[1])
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServer(grpcServer, w)
	go func() {
		log.Println("start listen")
		if err := grpcServer.Serve(ln); err != nil {
			panic(err)
		}
	}()

	masterHandle, err := grpc.Dial(w.peers[0], grpc.WithInsecure())
	w.grpcHandlers[0] = masterHandle
	defer masterHandle.Close()
	if err != nil {
		log.Fatal(err)
	}

	registerClient := pb.NewMasterClient(masterHandle)

	response, err := registerClient.Register(context.Background(), &pb.RegisterRequest{WorkerIndex: int32(w.selfId)})
	if err != nil || !response.Ok {
		log.Println(err)
		log.Fatal("error for register!!")
	}

	// wait for stop
	<-w.stopChannel
	log.Println("finish task")
}
