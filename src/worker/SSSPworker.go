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
	"math"
	"net"
	"os"
	pb "protobuf"
	"strconv"
	"strings"
	"sync"
	"time"
	"tools"
	"sort"
)

func Generate(g graph.Graph) map[graph.ID]float64 {
	distance := make(map[graph.ID]float64)

	for id := range g.GetNodes() {
		distance[id] = math.MaxFloat64
	}
	return distance
}

// rpc send has max size limit, so we spilt our transfer into many small block
func Peer2PeerSSSPSend(client pb.WorkerClient, message []*pb.SSSPMessageStruct, id int, calculateStep bool)  {
	for len(message) > tools.RPCSendSize {
		slice := message[0:tools.RPCSendSize]
		message = message[tools.RPCSendSize:]
		_, err := client.SSSPSend(context.Background(), &pb.SSSPMessageRequest{Pair: slice, CalculateStep:calculateStep})
		if err != nil {
			log.Printf("send to %v error\n", id)
			log.Fatal(err)
		}
	}
	if len(message) != 0 {
		_, err := client.SSSPSend(context.Background(), &pb.SSSPMessageRequest{Pair: message, CalculateStep:calculateStep})
		if err != nil {
			log.Printf("send to %v error\n", id)
			log.Fatal(err)
		}
	}
}

type SSSPWorker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	grpcHandlers map[int]*grpc.ClientConn
	workerNum int

	g           graph.Graph
	distance    map[graph.ID]float64 //
	//exchangeMsg map[graph.ID]float64
	updatedBuffer     []*algorithm.Pair
	exchangeBuffer    []*algorithm.Pair
	updatedMaster     map[graph.ID]bool
	updatedMirror     map[graph.ID]bool
	updatedByMessage  map[graph.ID]bool

	iterationNum int
	stopChannel  chan bool
}

func (w *SSSPWorker) Lock() {
	w.mutex.Lock()
}

func (w *SSSPWorker) UnLock() {
	w.mutex.Unlock()
}

func (w *SSSPWorker) ShutDown(ctx context.Context, args *pb.ShutDownRequest) (*pb.ShutDownResponse, error) {
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

func (w *SSSPWorker) SSSPMessageSend(messages map[int][]*algorithm.Pair, calculateStep bool) {
	SlicePeerSend := make([]*pb.WorkerCommunicationSize, 0)
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
		if indexBuffer[i] > w.selfId {
			start = i
			break
		}
	}
	indexBuffer = append(indexBuffer[start:], indexBuffer[:start]...)

	for i := 1; i <= batch; i++ {
		for j := (i - 1) * tools.ConnPoolSize; j < i * tools.ConnPoolSize && j < len(indexBuffer); j++ {
			partitionId := indexBuffer[j]
			message := messages[partitionId]
			wg.Add(1)

			eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{WorkerID:int32(partitionId + 1), CommunicationSize:int32(len(message))}
			SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)

			go func(partitionId int, message []*algorithm.Pair) {
				defer wg.Done()
				workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
				if err != nil {
					log.Fatal(err)
				}
				defer workerHandle.Close()

				client := pb.NewWorkerClient(workerHandle)
				encodeMessage := make([]*pb.SSSPMessageStruct, 0)
				for _, msg := range message {
					encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.IntVal(), Distance:msg.Distance})
				}
				Peer2PeerSSSPSend(client, encodeMessage, partitionId + 1, calculateStep)
			}(partitionId, message)

		}
		wg.Wait()
	}
}

func (w *SSSPWorker) peval(args *pb.PEvalRequest, id int) {
	var fullSendStart time.Time
	var fullSendDuration float64
	SlicePeerSend := make([]*pb.WorkerCommunicationSize, 0)
	startId := graph.ID(-1)

	if w.selfId == 1 {
		log.Println("my rank is 1")
		for v := range w.g.GetNodes() {
			startId = v
			break
		}
		//startId = graph.ID(3)
	}
	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum := algorithm.SSSP_PEVal(w.g, w.distance, startId)

	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place. contains nothing, client end should ignore it

		masterHandle := w.grpcHandlers[0]
		Client := pb.NewMasterClient(masterHandle)

		finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
			AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
			CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: 0,
			PairNum: SlicePeerSendNull, WorkerID: int32(id), MessageToSend: isMessageToSend}

		Client.SuperStepFinish(context.Background(), finishRequest)
	} else {
		fullSendStart = time.Now()
		w.SSSPMessageSend(messages, true)
	}

	fullSendDuration = time.Since(fullSendStart).Seconds()

	masterHandle := w.grpcHandlers[0]
	Client := pb.NewMasterClient(masterHandle)

	finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
		AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

	Client.SuperStepFinish(context.Background(), finishRequest)
}

func (w *SSSPWorker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
	go w.peval(args, w.selfId)
	return &pb.PEvalResponse{Ok:true}, nil
}

func (w *SSSPWorker) incEval(args *pb.IncEvalRequest, id int) {
	w.iterationNum++

	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, aggregateTime,
	aggregatorOriSize, aggregatorReducedSize := algorithm.SSSP_IncEval(w.g, w.distance, w.exchangeBuffer, w.updatedMaster, w.updatedMirror, w.updatedByMessage)

	log.Printf("zs-log: isMessageToSend:%v\n", isMessageToSend)

	w.exchangeBuffer = make([]*algorithm.Pair, 0)
	w.updatedMirror = make(map[graph.ID]bool)
	var fullSendStart time.Time
	var fullSendDuration float64
	SlicePeerSend := make([]*pb.WorkerCommunicationSize, 0)
	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place, contains nothing

		masterHandle := w.grpcHandlers[0]
		Client := pb.NewMasterClient(masterHandle)

		finishRequest := &pb.FinishRequest{AggregatorOriSize: aggregatorOriSize,
			AggregatorSeconds: aggregateTime, AggregatorReducedSize: aggregatorReducedSize, IterationSeconds: iterationTime,
			CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: 0,
			PairNum: SlicePeerSendNull, WorkerID: int32(id), MessageToSend: isMessageToSend}

		Client.SuperStepFinish(context.Background(), finishRequest)

	} else {
		fullSendStart = time.Now()
		w.SSSPMessageSend(messages, true)
	}
	fullSendDuration = time.Since(fullSendStart).Seconds()

	masterHandle := w.grpcHandlers[0]
	Client := pb.NewMasterClient(masterHandle)

	finishRequest := &pb.FinishRequest{AggregatorOriSize: aggregatorOriSize,
		AggregatorSeconds: aggregateTime, AggregatorReducedSize: aggregatorReducedSize, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

	Client.SuperStepFinish(context.Background(), finishRequest)
}

func (w *SSSPWorker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	go w.incEval(args, w.selfId)
	return &pb.IncEvalResponse{Update:true}, nil
}

func (w *SSSPWorker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	var f *os.File
	if tools.WorkerOnSC {
		f, _ = os.Create(tools.ResultPath + "ssspresult_" + strconv.Itoa(w.selfId-1))
	} else {
		f, _ = os.Create(tools.ResultPath + "/result_" + strconv.Itoa(w.selfId-1))
	}
	writer := bufio.NewWriter(f)
	defer f.Close()

	for id, dist := range w.distance {
	//	if w.g.IsMaster(id) {
			writer.WriteString(id.String() + "\t" + strconv.FormatFloat(dist, 'E', -1, 64) + "\n")
	//	}
	}
	writer.Flush()

	return &pb.AssembleResponse{Ok: true}, nil
}

func (w *SSSPWorker) ExchangeMessage(ctx context.Context, args *pb.ExchangeRequest) (*pb.ExchangeResponse, error) {
	//updated := make(map[graph.ID]bool)

	for _, pair := range w.updatedBuffer {
		id := pair.NodeId
		dis := pair.Distance

		log.Printf("updated buffer: id:%v, dis:%v\n", id, dis)

		if dis >= w.distance[id] {
			continue
		}

		w.distance[id] = dis
		w.updatedByMessage[id] = true
		//log.Printf("updated id: %v\n", id)
		if w.g.IsMaster(id) {
			w.updatedMaster[id] = true
		}
	}
	w.updatedBuffer = make([]*algorithm.Pair, 0)

	master := w.g.GetMasters()
	messageMap := make(map[int][]*algorithm.Pair)
	for id := range w.updatedMaster {
		for _, partition := range master[id] {
			if _, ok := messageMap[partition]; !ok {
				messageMap[partition] = make([]*algorithm.Pair, 0)
			}
			messageMap[partition] = append(messageMap[partition], &algorithm.Pair{NodeId: id, Distance: w.distance[id]})
		}
	}

	w.SSSPMessageSend(messageMap, false)
	w.updatedMaster = make(map[graph.ID]bool)

	return &pb.ExchangeResponse{Ok:true}, nil
}

func (w *SSSPWorker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
//	log.Println("send receive")
	decodeMessage := make([]*algorithm.Pair, 0)

	for _, msg := range args.Pair {
		decodeMessage = append(decodeMessage, &algorithm.Pair{NodeId: graph.ID(msg.NodeID), Distance: msg.Distance})
		log.Printf("received msg: nodeId:%v dis:%v\n", graph.ID(msg.NodeID), msg.Distance)
	}
	w.Lock()
	if args.CalculateStep {
		w.updatedBuffer = append(w.updatedBuffer, decodeMessage...)
	} else {
		w.exchangeBuffer = append(w.exchangeBuffer, decodeMessage...)
	}
	w.UnLock()

	return &pb.SSSPMessageResponse{}, nil
}

func (w *SSSPWorker) SimSend(ctx context.Context, args *pb.SimMessageRequest) (*pb.SimMessageResponse, error) {
	return nil, nil
}
func (w *SSSPWorker) PRSend(ctx context.Context, args *pb.PRMessageRequest) (*pb.PRMessageResponse, error) {
	return nil, nil
}

func newWorker(id, partitionNum int) *SSSPWorker {
	w := new(SSSPWorker)
	w.mutex = new(sync.Mutex)
	w.selfId = id
	w.peers = make([]string, 0)
	w.updatedBuffer = make([]*algorithm.Pair, 0)
	w.exchangeBuffer = make([]*algorithm.Pair, 0)
	w.updatedMaster = make(map[graph.ID]bool)
	w.updatedMirror = make(map[graph.ID]bool)
	w.updatedByMessage = make(map[graph.ID]bool)
	w.iterationNum = 0
	w.stopChannel = make(chan bool)
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

	w.workerNum = partitionNum
	start := time.Now()

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

	loadTime := time.Since(start)
	fmt.Printf("loadGraph Time: %v", loadTime)
	log.Printf("graph size:%v\n", len(w.g.GetNodes()))

	if w.g == nil {
		log.Println("can't load graph")
	}
	w.distance = Generate(w.g)

	return w
}

func RunWorker(id, partitionNum int) {
	w := newWorker(id, partitionNum)

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
		log.Fatal("error for register")
	}

	// wait for stop
	<-w.stopChannel
	log.Println("finish task")
}
