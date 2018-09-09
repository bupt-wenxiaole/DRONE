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
	sim     map[int64]Set.Set
	updatedMirror Set.Set
	updatedMaster Set.Set
	updatedByMessage Set.Set

	postMap map[int64]map[int64]int

	//edge_count int64

	calMessages map[int64]map[int64]int
	exchangeMessages map[int64]map[int64]int

	iterationNum int64
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
func Peer2PeerSimSend(client pb.WorkerClient, message []*pb.SimMessageStruct, partitionId int, srcId int, calculateStep bool)  {
	for len(message) > tools.RPCSendSize {
		slice := message[0:tools.RPCSendSize]
		message = message[tools.RPCSendSize:]
		_, err := client.SimSend(context.Background(), &pb.SimMessageRequest{Pair: slice, CalculateStep:calculateStep})
		if err != nil {
			log.Printf("%v send to %v error", srcId, partitionId)
			log.Fatal(err)
		}
	}
	if len(message) != 0 {
		_, err := client.SimSend(context.Background(), &pb.SimMessageRequest{Pair: message, CalculateStep:calculateStep})
		if err != nil {
			log.Printf("%v send to %v error", srcId, partitionId)
			log.Fatal(err)
		}
	}
}

func (w *SimWorker) SimMessageSend(messageMap map[int]map[algorithm.SimPair]int, calculateStep bool) []*pb.WorkerCommunicationSize {
	var wg sync.WaitGroup
	messageLen := len(messageMap)
	batch := (messageLen + tools.ConnPoolSize - 1) / tools.ConnPoolSize

	indexBuffer := make([]int, 0)
	for partitionId := range messageMap {
		indexBuffer = append(indexBuffer, partitionId)
	}
	sort.Ints(indexBuffer)
	start := 0
	for i := 1; i < len(indexBuffer); i++ {
		if indexBuffer[i] > w.selfId {
			start = i
			break
		}
	}
	indexBuffer = append(indexBuffer[start:], indexBuffer[:start]...)

	SlicePeerSend := make([]*pb.WorkerCommunicationSize, 0)
	for i := 1; i <= batch; i++ {
		for j := (i - 1) * tools.ConnPoolSize; j < i*tools.ConnPoolSize && j < len(indexBuffer); j++ {
			partitionId := indexBuffer[j]
			message := messageMap[partitionId]
			wg.Add(1)
			eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{WorkerID: int32(partitionId + 1), CommunicationSize: int32(len(message))}
			SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)

			go func(partitionId int, messageMap map[algorithm.SimPair]int) {
				defer wg.Done()
				workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
				if err != nil {
					log.Fatal(err)
				}
				defer workerHandle.Close()

				client := pb.NewWorkerClient(workerHandle)
				encodeMessage := make([]*pb.SimMessageStruct, 0)
				for pair, times := range message {
					encodeMessage = append(encodeMessage, &pb.SimMessageStruct{PatternId: pair.PatternNode, DataId: pair.DataNode, Times:int32(times)})
				}
				Peer2PeerSimSend(client, encodeMessage, partitionId+1, w.selfId, calculateStep)
			}(partitionId, message)

		}
		wg.Wait()
	}
	return SlicePeerSend
}

func (w *SimWorker) peVal(args *pb.PEvalRequest, id int) {
	var fullSendStart time.Time
	var fullSendDuration float64
	var SlicePeerSend []*pb.WorkerCommunicationSize

	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum := algorithm.GraphSim_PEVal(w.g, w.pattern, w.sim, w.postMap, w.updatedMaster, w.updatedMirror)
	w.updatedMirror = Set.NewSet()
/*
	for v := range w.g.GetNodes() {
		for u, times := range w.postMap[v] {
			log.Printf("after peval, u: %v, v: %v, time:%v\n", u.IntVal(), v.IntVal(), times)
		}
	}
*/
	for v := range w.g.GetNodes() {
		w.updatedByMessage.Add(v)
	}

	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place. contains nothing, client end should ignore it

		masterHandle := w.grpcHandlers[0]
		Client := pb.NewMasterClient(masterHandle)

		finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
			AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
			CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: int32(dstPartitionNum), AllPeerSend: 0,
			PairNum: SlicePeerSendNull, WorkerID: int32(id), MessageToSend: isMessageToSend}

		Client.SuperStepFinish(context.Background(), finishRequest)
		return
	} else {
		fullSendStart = time.Now()
		SlicePeerSend = w.SimMessageSend(messages, true)
	}
	fullSendDuration = time.Since(fullSendStart).Seconds()

	masterHandle := w.grpcHandlers[0]
	Client := pb.NewMasterClient(masterHandle)

	finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
		AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: int32(dstPartitionNum), AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

	Client.SuperStepFinish(context.Background(), finishRequest)
	return
}

func (w *SimWorker) ExchangeMessage(ctx context.Context, args *pb.ExchangeRequest) (*pb.ExchangeResponse, error) {
	for v, posts := range w.calMessages {
		for u, val := range posts {
			if w.postMap[v] == nil {
				w.postMap[v] = make(map[int64]int)
			}
			w.postMap[v][u] = w.postMap[v][u] + val
		}

		w.updatedByMessage.Add(v)
		w.updatedMaster.Add(v)
	}
	w.calMessages = make(map[int64]map[int64]int)

	messageMap := make(map[int]map[algorithm.SimPair]int)
	masterMap := w.g.GetMasters()
	for v := range w.updatedMaster {
		for u := range w.postMap[v] {
			if w.postMap[v][u] == 0 {
				delete(w.postMap[v], u)
				continue
			}
			for partitionId := range masterMap[v] {
				if _, ok := messageMap[partitionId]; !ok {
					messageMap[partitionId] = make(map[algorithm.SimPair]int)
				}
				simPair := algorithm.SimPair{DataNode:v, PatternNode:u}
				messageMap[partitionId][simPair] = w.postMap[v][u]
			}
		}
	}

	w.SimMessageSend(messageMap, false)
	w.updatedMaster = Set.NewSet()
	return &pb.ExchangeResponse{Ok:true}, nil
}

func (w *SimWorker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
	go w.peVal(args, w.selfId)
	return &pb.PEvalResponse{Ok:true}, nil
}

func (w *SimWorker) incEval(args *pb.IncEvalRequest, id int) {
	w.iterationNum++

	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, aggregateTime,
	aggregatorOriSize, aggregatorReducedSize := algorithm.GraphSim_IncEval(w.g, w.pattern, w.sim, w.postMap, w.updatedMaster, w.updatedByMessage, w.exchangeMessages)

	w.updatedByMessage = Set.NewSet()
	w.exchangeMessages = make(map[int64]map[int64]int)

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
		SlicePeerSend = w.SimMessageSend(messages, true)
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
	log.Printf("start IncEval, updated vertex size:%v\n", len(w.updatedByMessage))
	go w.incEval(args, w.selfId)
	return &pb.IncEvalResponse{Update:true}, nil
}

func (w *SimWorker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	log.Println("assemble!")

	var f *os.File
	if tools.WorkerOnSC {
		f, _ = os.Create(tools.ResultPath + "simresult_" + strconv.Itoa(w.selfId-1))
	} else {
		f, _ = os.Create(tools.ResultPath + "simresult_" + strconv.Itoa(w.selfId-1))
	}
	defer f.Close()
	writer := bufio.NewWriter(f)

	//result := make([]string, 0)
	for v, simSets := range w.sim {
		if w.g.IsMirror(v) {
			continue
		}

		for u := range simSets {
			writer.WriteString(strconv.FormatInt(u,10) + "\t" + strconv.FormatInt(v,10) + "\n")
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
	//log.Println("sim send receive")

	w.Lock()
	if args.CalculateStep {
		for _, pair := range args.Pair {
			u := pair.PatternId
			v := pair.DataId
			if _, ok := w.calMessages[v]; !ok {
				w.calMessages[v] = make(map[int64]int)
			}
			w.calMessages[v][u] = w.calMessages[v][u] + int(pair.Times)
		}
	} else {
		for _, pair := range args.Pair {
			u := pair.PatternId
			v := pair.DataId
			if _, ok := w.exchangeMessages[v]; !ok {
				w.exchangeMessages[v] = make(map[int64]int)
			}
			w.exchangeMessages[v][u] = w.exchangeMessages[v][u] + int(pair.Times)
		}
	}
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
	w.calMessages = make(map[int64]map[int64]int)
	w.exchangeMessages = make(map[int64]map[int64]int)
	w.sim = make(map[int64]Set.Set)
	w.grpcHandlers = make(map[int]*grpc.ClientConn)
	w.postMap = make(map[int64]map[int64]int)
	w.updatedMirror = Set.NewSet()
	w.updatedMaster = Set.NewSet()
	w.updatedByMessage = Set.NewSet()

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

	var graphIO, master, mirror, isolated *os.File

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
		isolated, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/Isolateds." + strconv.Itoa(w.selfId-1))
	} else {
		master, _ = os.Open(tools.NFSPath + "Master." + strconv.Itoa(w.selfId-1))
		mirror, _ = os.Open(tools.NFSPath + "Mirror." + strconv.Itoa(w.selfId-1))
		isolated, _ = os.Open(tools.NFSPath + "Isolateds." + strconv.Itoa(w.selfId-1))
	}
	defer master.Close()
	defer mirror.Close()
	defer isolated.Close()

	w.g, err = graph.NewGraphFromTXT(graphIO, master, mirror, isolated, false, false)
	if err != nil {
		log.Fatal(err)
	}

	patternFile, err := os.Open(tools.PatternPath)
	if err != nil {
		log.Fatal("pattern path error")
	}
	defer patternFile.Close()
	w.pattern, _ = graph.NewPatternGraph(patternFile)

	allPatternColor := make(map[int64]bool)

	//log.Printf("zs-log: start PEval initial for Pattern Node for rank:%v \n", id)
	//log.Printf("pattern node size:%v\n", patternNodeSet.Size())
	//log.Printf("zs-log: start delete useless node\n")
	for _, node := range w.pattern.GetNodes() {
		allPatternColor[node.Attr()] = true
	}

	for id, node := range w.g.GetNodes() {
		if _, ok := allPatternColor[node.Attr()]; !ok {
			w.g.DeleteNode(id)
		}
	}


	loadTime := time.Since(start)
	log.Printf("loadGraph Time: %v\n", loadTime)
	//fmt.Printf("loadGraph Time: %v\n", loadTime)

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
