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

func Generate(g graph.Graph) (map[graph.ID]float64, map[graph.ID]float64) {
	distance := make(map[graph.ID]float64)
	exchangeMsg := make(map[graph.ID]float64)

	for id := range g.GetNodes() {
		distance[id] = math.MaxFloat64
	}

	for id := range g.GetFOs() {
		exchangeMsg[id] = math.MaxFloat64
	}
	return distance, exchangeMsg
}

// rpc send has max size limit, so we spilt our transfer into many small block
func Peer2PeerSSSPSend(client pb.WorkerClient, message []*pb.SSSPMessageStruct, id int)  {

	for len(message) > tools.RPCSendSize {
		slice := message[0:tools.RPCSendSize]
		message = message[tools.RPCSendSize:]
		_, err := client.SSSPSend(context.Background(), &pb.SSSPMessageRequest{Pair: slice})
		if err != nil {
			log.Printf("send to %v error\n", id)
			log.Fatal(err)
		}
	}
	if len(message) != 0 {
		_, err := client.SSSPSend(context.Background(), &pb.SSSPMessageRequest{Pair: message})
		if err != nil {
			log.Printf("send to %v error\n", id)
			log.Fatal(err)
		}
	}
}

type Worker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	grpcHandlers map[int]*grpc.ClientConn
	workerNum int

	g           graph.Graph
	distance    map[graph.ID]float64 //
	exchangeMsg map[graph.ID]float64
	updated     []*algorithm.Pair

	routeTable map[graph.ID][]*algorithm.BoundMsg

	iterationNum int
	stopChannel  chan bool
}

func (w *Worker) Lock() {
	w.mutex.Lock()
}

func (w *Worker) UnLock() {
	w.mutex.Unlock()
}

func (w *Worker) ShutDown(ctx context.Context, args *pb.ShutDownRequest) (*pb.ShutDownResponse, error) {
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

func (w * Worker) peval(args *pb.PEvalRequest, id int)  {
	var fullSendStart time.Time
	var fullSendDuration float64
	var SlicePeerSend []*pb.WorkerCommunicationSize
	var startId graph.ID = graph.StringID(-1)
	if w.selfId == 1 {
		log.Println("my rank is 1")
		for v := range w.g.GetNodes() {
			startId = v
			break
		}
	}
	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum := algorithm.SSSP_PEVal(w.g, w.distance, w.exchangeMsg, w.routeTable, startId)
	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place. contains nothing, client end should ignore it

		/*
		masterHandle, err := grpc.Dial(w.peers[0], grpc.WithInsecure())
		if err != nil {
			log.Fatal(err)
		}
		defer masterHandle.Close()
		*/
		masterHandle := w.grpcHandlers[0]
		Client := pb.NewMasterClient(masterHandle)

		finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
			AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
			CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: 0,
			PairNum: SlicePeerSendNull, WorkerID: int32(id), MessageToSend: isMessageToSend}

		Client.SuperStepFinish(context.Background(), finishRequest)
	} else {
		fullSendStart = time.Now()
		var wg sync.WaitGroup
		messageLen := len(messages)
		batch := (messageLen + tools.ConnPoolSize - 1) / tools.ConnPoolSize
		//messageSlice := make([])

		indexBuffer := make([]int, messageLen)
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
				go func(partitionId int, message []*algorithm.Pair) {
					defer wg.Done()
					workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
					if err != nil {
						log.Fatal(err)
					}
					defer workerHandle.Close()

					client := pb.NewWorkerClient(workerHandle)
					encodeMessage := make([]*pb.SSSPMessageStruct, 0)
					eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{int32(partitionId), int32(len(message))}
					SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)
					for _, msg := range message {
						encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.IntVal(), Distance:msg.Distance})
					}
					Peer2PeerSSSPSend(client, encodeMessage, partitionId + 1)
				}(partitionId, message)

			}
			wg.Wait()
		}
	}
	fullSendDuration = time.Since(fullSendStart).Seconds()
/*
	masterHandle, err := grpc.Dial(w.peers[0], grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
*/
	masterHandle := w.grpcHandlers[0]
	Client := pb.NewMasterClient(masterHandle)
	defer masterHandle.Close()

	finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
		AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

	Client.SuperStepFinish(context.Background(), finishRequest)
}

func (w *Worker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
	go w.peval(args, w.selfId)
	return &pb.PEvalResponse{}, nil
}

func (w *Worker) incEval(args *pb.IncEvalRequest, id int) {
	w.iterationNum++
	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, aggregateTime,
	aggregatorOriSize, aggregatorReducedSize := algorithm.SSSP_IncEval(w.g, w.distance, w.exchangeMsg, w.routeTable, w.updated)

	w.updated = make([]*algorithm.Pair, 0)
	var fullSendStart time.Time
	var fullSendDuration float64
	var SlicePeerSend []*pb.WorkerCommunicationSize
	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place, contains nothing

		/*masterHandle, err := grpc.Dial(w.peers[0], grpc.WithInsecure())
		if err != nil {
			log.Fatal(err)
		}
		defer masterHandle.Close()
		*/
		masterHandle := w.grpcHandlers[0]
		Client := pb.NewMasterClient(masterHandle)

		finishRequest := &pb.FinishRequest{AggregatorOriSize: aggregatorOriSize,
			AggregatorSeconds: aggregateTime, AggregatorReducedSize: aggregatorReducedSize, IterationSeconds: iterationTime,
			CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: 0,
			PairNum: SlicePeerSendNull, WorkerID: int32(id), MessageToSend: isMessageToSend}

		Client.SuperStepFinish(context.Background(), finishRequest)

	} else {
		fullSendStart = time.Now()
		var wg sync.WaitGroup

		messageLen := len(messages)
		batch := (messageLen + tools.ConnPoolSize - 1) / tools.ConnPoolSize

		indexBuffer := make([]int, 0)
		for partitionId := range messages {
			indexBuffer = append(indexBuffer, partitionId)
			//log.Printf("zs-log: self id:%v, partitionId:%v\n", id, partitionId)
		}
		sort.Ints(indexBuffer)
		//log.Printf("zs-log: self id:%v, partitionId:%v\n", id, indexBuffer[0])
		start := 0
		for i := 1; i < len(indexBuffer); i++ {
			if indexBuffer[i] > id {
				start = i
				break
			}
		}
		for _, i := range indexBuffer {
				log.Printf("zs-log: self id:%v, partitionId:%v\n", id, i)
		}
		indexBuffer = append(indexBuffer[start:], indexBuffer[:start]...)
		//for _, i := range indexBuffer {
		//	log.Printf("zs-log: self id:%v, partitionId:%v\n", id, i)
		//}

		for i := 1; i <= batch; i++ {
			for j := (i - 1) * tools.ConnPoolSize; j < i * tools.ConnPoolSize && j < len(indexBuffer); j++ {
				wg.Add(1)
				partitionId := indexBuffer[j]
				message := messages[partitionId]
				go func(partitionId int, message []*algorithm.Pair) {
					defer wg.Done()
				//	log.Printf("id:%v\n", partitionId + 1)
					workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
					if err != nil {
						log.Fatal(err)
					}
					defer workerHandle.Close()

					client := pb.NewWorkerClient(workerHandle)
					encodeMessage := make([]*pb.SSSPMessageStruct, 0)
					eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{int32(partitionId), int32(len(message))}
					SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)
					for _, msg := range message {
						encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.IntVal(), Distance: msg.Distance})
					}
					Peer2PeerSSSPSend(client, encodeMessage, partitionId + 1)
				}(partitionId, message)

			}
			wg.Wait()
		}
	}
	fullSendDuration = time.Since(fullSendStart).Seconds()

	masterHandle, err := grpc.Dial(w.peers[0], grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	Client := pb.NewMasterClient(masterHandle)
	defer masterHandle.Close()

	finishRequest := &pb.FinishRequest{AggregatorOriSize: aggregatorOriSize,
		AggregatorSeconds: aggregateTime, AggregatorReducedSize: aggregatorReducedSize, IterationSeconds: iterationTime,
		CombineSeconds: combineTime, IterationNum: iterationNum, UpdatePairNum: updatePairNum, DstPartitionNum: dstPartitionNum, AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

	Client.SuperStepFinish(context.Background(), finishRequest)
}

func (w *Worker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	go w.incEval(args, w.selfId)
	return &pb.IncEvalResponse{}, nil
}

func (w *Worker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	var f *os.File
	if tools.WorkerOnSC {
		f, _ = os.Create(tools.ResultPath + strconv.Itoa(w.workerNum) + "/result_" + strconv.Itoa(w.selfId-1))
	} else {
		f, _ = os.Create(tools.ResultPath + "/result_" + strconv.Itoa(w.selfId-1))
	}
	writer := bufio.NewWriter(f)
	defer writer.Flush()
	defer f.Close()

	for id, dist := range w.distance {
		writer.WriteString(id.String() + "\t" + strconv.FormatFloat(dist, 'E', -1, 64) + "\n")
	}

	return &pb.AssembleResponse{Ok: true}, nil
}

func (w *Worker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
//	log.Println("send receive")
	decodeMessage := make([]*algorithm.Pair, 0)

	for _, msg := range args.Pair {
		decodeMessage = append(decodeMessage, &algorithm.Pair{NodeId: graph.StringID(msg.NodeID), Distance: msg.Distance})
		//log.Printf("received msg: nodeId:%v dis:%v\n", graph.StringID(msg.NodeID), msg.Distance)
	}
	w.Lock()
	w.updated = append(w.updated, decodeMessage...)
	w.UnLock()

	return &pb.SSSPMessageResponse{}, nil
}

func (w *Worker) SimSend(ctx context.Context, args *pb.SimMessageRequest) (*pb.SimMessageResponse, error) {
	return nil, nil
}
func (w *Worker) PRSend(ctx context.Context, args *pb.PRMessageRequest) (*pb.PRMessageResponse, error) {
	return nil, nil
}

func newWorker(id, partitionNum int) *Worker {
	w := new(Worker)
	w.mutex = new(sync.Mutex)
	w.selfId = id
	w.peers = make([]string, 0)
	w.updated = make([]*algorithm.Pair, 0)
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
	if tools.LoadFromJson {
		graphIO, _ := os.Open(tools.NFSPath + "G" + strconv.Itoa(partitionNum) + "_" + strconv.Itoa(w.selfId-1) + ".json")
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}

		partitionIO, _ := os.Open(tools.NFSPath + "P" + strconv.Itoa(partitionNum) + "_" + strconv.Itoa(w.selfId-1) + ".json")
		defer partitionIO.Close()

		w.g, err = graph.NewGraphFromJSON(graphIO, partitionIO, strconv.Itoa(w.selfId-1))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var graphIO, fxiReader, fxoReader *os.File
		if tools.WorkerOnSC {
			graphIO, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "cores/G." + strconv.Itoa(w.selfId-1))
		} else {
			graphIO, _ = os.Open(tools.NFSPath + "G." + strconv.Itoa(w.selfId-1))
		}
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}
		if tools.WorkerOnSC {
			fxiReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "cores/F" + strconv.Itoa(w.selfId-1) + ".I")
			fxoReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "cores/F" + strconv.Itoa(w.selfId-1) + ".O")
		} else {
			fxiReader, _ = os.Open(tools.NFSPath + "F" + strconv.Itoa(w.selfId-1) + ".I")
			fxoReader, _ = os.Open(tools.NFSPath + "F" + strconv.Itoa(w.selfId-1) + ".O")
		}
		defer fxiReader.Close()
		defer fxoReader.Close()

		w.g, err = graph.NewGraphFromTXT(graphIO, fxiReader, fxoReader, strconv.Itoa(w.selfId-1))
		if err != nil {
			log.Fatal(err)
		}
	}

	loadTime := time.Since(start)
	fmt.Printf("loadGraph Time: %v", loadTime)

	if w.g == nil {
		log.Println("can't load graph")
	}
	// Initial some variables from graph
	w.routeTable = algorithm.GenerateRouteTable(w.g.GetFOs())
	w.distance, w.exchangeMsg = Generate(w.g)

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
