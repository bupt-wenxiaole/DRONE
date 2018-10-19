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
	"sort"
	"Set"
)

type PRWorker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	grpcHandlers map[int]*grpc.ClientConn

	g            graph.Graph
	prVal        map[int64]float64
	accVal       map[int64]float64
	updatedSet   Set.Set
	partitionNum int
	receiveBuffer map[int64]float64

	targetNum    map[int64]int

	iterationNum int
	stopChannel  chan bool
}

func (w *PRWorker) Lock() {
	w.mutex.Lock()
}

func (w *PRWorker) UnLock() {
	w.mutex.Unlock()
}

// rpc send has max size limit, so we spilt our transfer into many small block
func Peer2PeerPRSend(client pb.WorkerClient, message []*pb.PRMessageStruct)  {

	for len(message) > tools.RPCSendSize {
		slice := message[0:tools.RPCSendSize]
		message = message[tools.RPCSendSize:]
		_, err := client.PRSend(context.Background(), &pb.PRMessageRequest{Pair: slice})
		if err != nil {
			log.Fatal(err)
		}
	}
	if len(message) != 0 {
		_, err := client.PRSend(context.Background(), &pb.PRMessageRequest{Pair: message})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (w *PRWorker) ShutDown(ctx context.Context, args *pb.ShutDownRequest) (*pb.ShutDownResponse, error) {
	log.Println("receive shutDown request")
	w.Lock()
	defer w.UnLock()
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

func (w *PRWorker) PRMessageSend(messages map[int][]*algorithm.PRPair) []*pb.WorkerCommunicationSize {
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

			go func(partitionId int, message []*algorithm.PRPair) {
				defer wg.Done()
				workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
				if err != nil {
					log.Fatal(err)
				}
				defer workerHandle.Close()

				client := pb.NewWorkerClient(workerHandle)
				encodeMessage := make([]*pb.PRMessageStruct, 0)
				for _, msg := range message {
					encodeMessage = append(encodeMessage, &pb.PRMessageStruct{NodeID:msg.ID.IntVal(), PrVal:msg.PRValue})
				}
				Peer2PeerPRSend(client, encodeMessage)
			}(partitionId, message)
		}
		wg.Wait()
	}
	return SlicePeerSend
}

func (w * PRWorker) peval(args *pb.PEvalRequest, id int) {
	var fullSendStart time.Time
	var fullSendDuration float64

	_, messagesMap, iterationTime := algorithm.PageRank_PEVal(w.g, w.targetNum, w.prVal, w.accVal, w.updatedSet)

	dstPartitionNum := len(messagesMap)

	fullSendStart = time.Now()
	SlicePeerSend := w.PRMessageSend(messagesMap)
	fullSendDuration = time.Since(fullSendStart).Seconds()

	masterHandle := w.grpcHandlers[0]
	Client := pb.NewMasterClient(masterHandle)

	finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
		AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
		CombineSeconds: 0, IterationNum: 0, UpdatePairNum: 0, DstPartitionNum: int32(dstPartitionNum), AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: true}

	Client.SuperStepFinish(context.Background(), finishRequest)
}

func (w *PRWorker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
	go w.peval(args, w.selfId)
	return &pb.PEvalResponse{Ok: true}, nil
}

func (w *PRWorker) incEval(args *pb.IncEvalRequest, id int) {
	w.mutex.Lock()
	tempBuffer := w.receiveBuffer
	w.receiveBuffer = make(map[int64]float64)
	w.mutex.Unlock()

	w.iterationNum++

	isMessageToSend, messagesMap, iterationTime := algorithm.PageRank_IncEval(w.g, w.targetNum, w.prVal, w.accVal,w.updatedSet, tempBuffer)

	dstPartitionNum := len(messagesMap)

	fullSendStart := time.Now()
	SlicePeerSend := w.PRMessageSend(messagesMap)
	fullSendDuration := time.Since(fullSendStart).Seconds()

	masterHandle := w.grpcHandlers[0]
	Client := pb.NewMasterClient(masterHandle)

	finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
		AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
		CombineSeconds: 0, IterationNum: 0, UpdatePairNum: 0, DstPartitionNum: int32(dstPartitionNum), AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend, WorkerID: int32(id), MessageToSend: isMessageToSend}

	Client.SuperStepFinish(context.Background(), finishRequest)
}

func (w *PRWorker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	go w.incEval(args, w.selfId)
	return &pb.IncEvalResponse{true}, nil
}

func (w *PRWorker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	f, err:= os.Create(tools.ResultPath + "result_" + strconv.Itoa(w.selfId - 1))
	if err != nil {
		log.Panic(err)
	}
	writer := bufio.NewWriter(f)
	defer writer.Flush()
	defer f.Close()

	for id, pr := range w.prVal {
		writer.WriteString(strconv.FormatInt(id, 10) +"\t"+strconv.FormatFloat(pr, 'E', -1, 64) + "\n")
	}
	return &pb.AssembleResponse{Ok: true}, nil
}

func (w *PRWorker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
	return nil, nil
}
func (w *PRWorker) SimSend(ctx context.Context, args *pb.SimMessageRequest) (*pb.SimMessageResponse, error) {
	return nil, nil
}

func (w *PRWorker) PRSend(ctx context.Context, args *pb.PRMessageRequest) (*pb.PRMessageResponse, error) {
	w.Lock()
	for _, msg := range args.Pair {
		w.receiveBuffer[msg.NodeID] += msg.PrVal
	//	log.Printf("received msg: nodeId:%v prVal:%v\n", graph.StringID(msg.NodeID), msg.PrVal)
	}
	w.UnLock()

	return &pb.PRMessageResponse{}, nil
}

func newPRWorker(id, partitionNum int) *PRWorker {
	w := new(PRWorker)
	w.mutex = new(sync.Mutex)
	w.selfId = id
	w.peers = make([]string, 0)
	w.iterationNum = 0
	w.stopChannel = make(chan bool)
	w.prVal = make(map[int64]float64)
	w.accVal = make(map[int64]float64)
	w.partitionNum = partitionNum
	w.receiveBuffer = make(map[int64]float64, 0)
	w.updatedSet = Set.NewSet()
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

	if tools.LoadFromJson {
		graphIO, _ := os.Open(tools.NFSPath + "G" + strconv.Itoa(partitionNum) + "_" + strconv.Itoa(w.selfId-1) + ".json")
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}
		partitionIO, _ := os.Open(tools.NFSPath + "P" + strconv.Itoa(partitionNum) + "_" + strconv.Itoa(w.selfId-1) + ".json")
		//partitionIO, _ := os.Open(tools.PartionPath)
		defer partitionIO.Close()

		w.g, err = graph.NewGraphFromJSON(graphIO, partitionIO, strconv.Itoa(w.selfId-1))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var graphIO, fxiReader, fxoReader *os.File
		if tools.WorkerOnSC {
			graphIO, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "cores/G." + strconv.Itoa(w.selfId-1))
			//graphIO, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/G." + strconv.Itoa(w.selfId-1))
		} else {
			graphIO, _ = os.Open(tools.NFSPath + "G." + strconv.Itoa(w.selfId-1))
		}
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}
		if tools.WorkerOnSC {
			fxoReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "cores/F" + strconv.Itoa(w.selfId-1) + ".O")
			//fxiReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/F" + strconv.Itoa(w.selfId-1) + ".I")
			//fxoReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/F" + strconv.Itoa(w.selfId-1) + ".O")
		} else {
			fxiReader, _ = os.Open(tools.NFSPath + "F" + strconv.Itoa(w.selfId-1) + ".I")
			fxoReader, _ = os.Open(tools.NFSPath + "F" + strconv.Itoa(w.selfId-1) + ".O")
		}
		defer fxiReader.Close()
		defer fxoReader.Close()

		w.g, err = graph.NewGraphFromTXT(graphIO, tools.NFSPath + "F" + strconv.Itoa(w.selfId-1) + ".O")
		if err != nil {
			log.Fatal(err)
		}
	}

	w.targetNum = algorithm.GenerateTarget(w.g)

	loadTime := time.Since(start)
	fmt.Printf("loadGraph Time: %v", loadTime)
	log.Printf("graph size:%v\n", len(w.g.GetNodes()))

	if w.g == nil {
		log.Println("can't load graph")
	}
	return w
}

func RunPRWorker(id, partitionNum int) {
	w := newPRWorker(id, partitionNum)

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
