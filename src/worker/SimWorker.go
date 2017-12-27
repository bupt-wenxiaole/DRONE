package worker

import (
	"algorithm"
	"bufio"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gopkg.in/fatih/set.v0"
	"graph"
	"io"
	"log"
	"math"
	"net"
	"os"
	pb "protobuf"
	"strconv"
	"strings"
	"sync"
	"time"
	"tools"
)

type SimWorker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	grpcHandlers []*grpc.ClientConn

	g       graph.Graph
	pattern graph.Graph
	sim     map[graph.ID]set.Interface
	preSet  map[graph.ID]set.Interface
	postSet map[graph.ID]set.Interface

	message []*algorithm.SimPair

	iterationNum int
	stopChannel  chan bool
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
	log.Println("shutdown ing")

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

func (w *SimWorker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
	// init grpc handler and store it
	// cause until now, we can ensure all workers have in work,
	// so we do this here

	w.grpcHandlers = make([]*grpc.ClientConn, len(w.peers))
	for id, peer := range w.peers {
		if id == w.selfId || id == 0 {
			continue
		}
		conn, err := grpc.Dial(peer, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		w.grpcHandlers[id] = conn
	}

	// Load graph data
	var fullSendStart time.Time
	var fullSendDuration float64
	var SlicePeerSend []*pb.WorkerCommunicationSize
	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum := algorithm.GraphSim_PEVal(w.g, w.pattern, w.sim, w.preSet, w.postSet)
	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place. contains nothing, client end should ignore it
		return &pb.PEvalResponse{Ok: isMessageToSend, Body: &pb.PEvalResponseBody{iterationNum, iterationTime,
			combineTime, updatePairNum, dstPartitionNum, 0, SlicePeerSendNull}}, nil
	} else {
		fullSendStart = time.Now()
		for partitionId, message := range messages {
			client := pb.NewWorkerClient(w.grpcHandlers[partitionId+1])
			encodeMessage := make([]*pb.SimMessageStruct, 0)
			eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{int32(partitionId), int32(len(message))}
			SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)
			for _, msg := range message {
				encodeMessage = append(encodeMessage, &pb.SimMessageStruct{PatternId: msg.PatternNode.String(), DataId: msg.DataNode.String()})
				//log.Printf("nodeId:%v dis:%v \n", msg.NodeId.String(), msg.Distance)
			}
			log.Printf("send partition id:%v\n", partitionId)
			_, err := client.SimSend(context.Background(), &pb.SimMessageRequest{Pair: encodeMessage})
			if err != nil {
				log.Println("send error")
				log.Fatal(err)
			}
		}
		fullSendDuration = time.Since(fullSendStart).Seconds()
	}
	return &pb.PEvalResponse{Ok: isMessageToSend, Body: &pb.PEvalResponseBody{IterationNum: iterationNum, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: fullSendDuration, PairNum: SlicePeerSend}}, nil
}

func (w *SimWorker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	w.iterationNum++
	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, aggregateTime,
		aggregatorOriSize, aggregatorReducedSize := algorithm.GraphSim_IncEval(w.g, w.pattern, w.sim, w.preSet, w.postSet, w.message)
	w.message = make([]*algorithm.SimPair, 0)
	var fullSendStart time.Time
	var fullSendDuration float64
	var SlicePeerSend []*pb.WorkerCommunicationSize
	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place, contains nothing
		return &pb.IncEvalResponse{Update: isMessageToSend, Body: &pb.IncEvalResponseBody{AggregatorOriSize: aggregatorOriSize,
			AggregatorSeconds: aggregateTime, AggregatorReducedSize: aggregatorReducedSize, IterationSeconds: iterationTime,
			CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: 0,
			PairNum: SlicePeerSendNull}}, nil
	} else {
		fullSendStart = time.Now()
		for partitionId, message := range messages {
			client := pb.NewWorkerClient(w.grpcHandlers[partitionId+1])
			encodeMessage := make([]*pb.SimMessageStruct, 0)
			eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{int32(partitionId), int32(len(message))}
			SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)
			for _, msg := range message {
				encodeMessage = append(encodeMessage, &pb.SimMessageStruct{PatternId: msg.PatternNode.String(), DataId: msg.DataNode.String()})
			}
			_, err := client.SimSend(context.Background(), &pb.SimMessageRequest{Pair: encodeMessage})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	fullSendDuration = time.Since(fullSendStart).Seconds()

	return &pb.IncEvalResponse{Update: isMessageToSend, Body: &pb.IncEvalResponseBody{AggregatorOriSize: aggregatorOriSize,
		AggregatorSeconds: aggregateTime, AggregatorReducedSize: aggregatorReducedSize, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend}}, nil
}

func (w *SimWorker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	fs := tools.GenerateAlluxioClient(tools.AlluxioHost)
	innerNodes := w.g.GetNodes()

	result := make([]string, 0)
	for u, simSets := range w.sim {
		for _, tmp := range simSets.List() {
			v := tmp.(graph.ID)
			if _, ok := innerNodes[v]; ok {
				result = append(result, u.String()+"\t"+v.String())
			}
		}
	}

	ok, err := tools.WriteToAlluxio(fs, tools.ResultPath+"result_"+strconv.Itoa(w.selfId), result)
	if err != nil {
		log.Panic(err)
	}

	return &pb.AssembleResponse{Ok: ok}, nil
}

/*
func (w *SimWorker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
	return nil, nil
}
*/

func (w *SimWorker) SimSend(ctx context.Context, args *pb.SimMessageRequest) (*pb.SimMessageResponse, error) {
	w.Lock()
	defer w.UnLock()

	for _, messagePair := range args.Pair {
		w.message = append(w.message, &algorithm.SimPair{DataNode: graph.StringID(messagePair.DataId), PatternNode: graph.StringID(messagePair.PatternId)})
	}
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

	suffix := strconv.Itoa(partitionNum) + "_"
	graphIO, _ := tools.ReadFromAlluxio(tools.GraphPath+"G"+suffix+strconv.Itoa(w.selfId-1)+".json", "G"+suffix+strconv.Itoa(w.selfId-1)+".json")
	defer tools.DeleteLocalFile("G" + suffix + strconv.Itoa(w.selfId-1) + ".json")
	defer graphIO.Close()

	if graphIO == nil {
		fmt.Println("graphIO is nil")
	}
	patternFile, _ := os.Open(tools.PatternPath)
	defer patternFile.Close()
	w.pattern, _ = graph.NewPatternGraph(patternFile)

	partitionIO, _ := tools.ReadFromAlluxio(tools.PartitionPath+"P"+suffix+strconv.Itoa(w.selfId-1)+".json", "P"+suffix+strconv.Itoa(w.selfId-1)+".json")
	defer tools.DeleteLocalFile("P" + suffix + strconv.Itoa(w.selfId-1) + ".json")
	defer partitionIO.Close()

	w.g, err = graph.NewGraphFromJSON(graphIO, partitionIO, strconv.Itoa(w.selfId-1))
	if err != nil {
		log.Fatal(err)
	}
	loadTime := time.Since(start)
	fmt.Printf("loadGraph Time: %vs", loadTime)

	if w.g == nil {
		log.Println("can't load graph")
	}
	// Initial some variables from graph
	w.preSet, w.postSet = algorithm.GeneratePrePostFISet(w.g)

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
	if err != nil {
		log.Fatal(err)
	}
	registerClient := pb.NewMasterClient(masterHandle)
	response, err := registerClient.Register(context.Background(), &pb.RegisterRequest{WorkerIndex: int32(w.selfId)})
	if err != nil || !response.Ok {
		log.Fatal("error for register")
	}

	// wait for stop
	<-w.stopChannel
	log.Println("finish task")
}
