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
	"time"
	"tools"
	"sync"
	"sort"
)

type PRWorker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	grpcHandlers map[int]*grpc.ClientConn

	g            graph.Graph
	prVal        map[int64]float64
	oldPr        map[int64]float64
	partitionNum int
	calBuffer    []*algorithm.PRPair
	exchangeBuffer []*algorithm.PRPair
	targetsNum   map[int64]int

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
func Peer2PeerPRSend(client pb.WorkerClient, message []*pb.PRMessageStruct, id int, calculateStep bool) {
	for len(message) > tools.RPCSendSize {
		slice := message[0:tools.RPCSendSize]
		message = message[tools.RPCSendSize:]
		_, err := client.PRSend(context.Background(), &pb.PRMessageRequest{Pair: slice, CalculateStep:calculateStep})
		if err != nil {
			log.Printf("send to %d error\n", id)
			log.Fatal(err)
		}
	}
	if len(message) != 0 {
		_, err := client.PRSend(context.Background(), &pb.PRMessageRequest{Pair: message, CalculateStep:calculateStep})
		if err != nil {
			log.Printf("send to %d error\n", id)
			log.Fatal(err)
		}
	}
}

func (w *PRWorker) PRMessageSend(messages map[int][]*algorithm.PRPair, calculateStep bool) []*pb.WorkerCommunicationSize {
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
				Peer2PeerPRSend(client, encodeMessage, partitionId + 1, calculateStep)
			}(partitionId, message)
		}
		wg.Wait()
	}
	return SlicePeerSend
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

func (w *PRWorker) ExchangeMessage(ctx context.Context, args *pb.ExchangeRequest) (*pb.ExchangeResponse, error) {
	for _, pair := range w.calBuffer {
		id := pair.ID
		pr := pair.PRValue

		w.prVal[id.IntVal()] = w.prVal[id.IntVal()] + pr
	}
	w.calBuffer = make([]*algorithm.PRPair, 0)

	master := w.g.GetMasters()
	messageMap := make(map[int][]*algorithm.PRPair)
	for id, partitions := range master {
		for _, partition := range partitions {
			if _, ok := messageMap[partition]; !ok {
				messageMap[partition] = make([]*algorithm.PRPair, 0)
			}
			messageMap[partition] = append(messageMap[partition], &algorithm.PRPair{ID: id, PRValue: w.prVal[id.IntVal()]})
		}
	}

	w.PRMessageSend(messageMap, false)

	return &pb.ExchangeResponse{Ok:true}, nil
}

func (w *PRWorker) peval(args *pb.PEvalRequest, id int) {
	var fullSendStart time.Time
	var fullSendDuration float64
	var SlicePeerSend []*pb.WorkerCommunicationSize

	_, messagesMap, iterationTime := algorithm.PageRank_PEVal(w.g, w.prVal, w.oldPr, w.targetsNum)

	dstPartitionNum := len(messagesMap)

	fullSendStart = time.Now()
	SlicePeerSend = w.PRMessageSend(messagesMap, true)
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
	return &pb.PEvalResponse{Ok:true}, nil
}

func (w *PRWorker) incEval(args *pb.IncEvalRequest, id int) {
	w.iterationNum++

	var isMessageToSend bool
	var messagesMap map[int][]*algorithm.PRPair

	var iterationTime float64

	isMessageToSend, messagesMap, w.oldPr, w.prVal, iterationTime = algorithm.PageRank_IncEval(w.g, w.prVal, w.oldPr, w.targetsNum, w.exchangeBuffer)

	w.exchangeBuffer = make([]*algorithm.PRPair, 0)

	if !isMessageToSend {
		var SlicePeerSendNull []*pb.WorkerCommunicationSize // this struct only for hold place, contains nothing

		masterHandle := w.grpcHandlers[0]
		Client := pb.NewMasterClient(masterHandle)

		finishRequest := &pb.FinishRequest{AggregatorOriSize: 0,
			AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: iterationTime,
			CombineSeconds: 0, IterationNum: 0, UpdatePairNum: 0, DstPartitionNum: 0, AllPeerSend: 0,
			PairNum: SlicePeerSendNull, WorkerID: int32(id), MessageToSend: isMessageToSend}

		Client.SuperStepFinish(context.Background(), finishRequest)
		return
	}

	dstPartitionNum := len(messagesMap)

	fullSendStart := time.Now()
	SlicePeerSend := w.PRMessageSend(messagesMap, true)
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
	return &pb.IncEvalResponse{Update:true}, nil
}

func (w *PRWorker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	log.Println("start assemble")
	f, err:= os.Create(tools.ResultPath + "PRresult_" + strconv.Itoa(w.selfId - 1))
	if err != nil {
		log.Panic(err)
	}
	writer := bufio.NewWriter(f)
	defer writer.Flush()
	defer f.Close()

	for id, pr := range w.prVal {
		if w.g.IsMirror(graph.ID(id)) {
			continue
		}
		writer.WriteString(strconv.FormatInt(id, 10) +"\t"+strconv.FormatFloat(pr, 'E', -1, 64) + "\n")
	}
	writer.Flush()
	return &pb.AssembleResponse{Ok: true}, nil
}

func (w *PRWorker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
	return nil, nil
}
func (w *PRWorker) SimSend(ctx context.Context, args *pb.SimMessageRequest) (*pb.SimMessageResponse, error) {
	return nil, nil
}

func (w *PRWorker) PRSend(ctx context.Context, args *pb.PRMessageRequest) (*pb.PRMessageResponse, error) {
	decodeMessage := make([]*algorithm.PRPair, 0)
	for _, msg := range args.Pair {
		decodeMessage = append(decodeMessage, &algorithm.PRPair{PRValue:msg.PrVal, ID:graph.ID(msg.NodeID)})
	}

	w.Lock()
	if args.CalculateStep {
		w.calBuffer = append(w.calBuffer, decodeMessage...)
	} else {
		w.exchangeBuffer = append(w.exchangeBuffer, decodeMessage...)
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
	w.prVal = make(map[int64]float64, 0)
	w.oldPr = make(map[int64]float64, 0)
	w.partitionNum = partitionNum
	w.calBuffer = make([]*algorithm.PRPair, 0)
	w.exchangeBuffer = make([]*algorithm.PRPair, 0)
	w.targetsNum = make(map[int64]int)
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

	var graphIO, master, mirror, isolated, targetsFile *os.File

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
		targetsFile, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/Degree." + strconv.Itoa(w.selfId-1))
	} else {
		master, _ = os.Open(tools.NFSPath + "Master." + strconv.Itoa(w.selfId-1))
		mirror, _ = os.Open(tools.NFSPath + "Mirror." + strconv.Itoa(w.selfId-1))
		isolated, _ = os.Open(tools.NFSPath + "Isolateds." + strconv.Itoa(w.selfId-1))
		targetsFile, _ = os.Open(tools.NFSPath + "Degree." + strconv.Itoa(w.selfId-1))
	}
	defer master.Close()
	defer mirror.Close()
	defer isolated.Close()
	defer targetsFile.Close()

	w.g, err = graph.NewGraphFromTXT(graphIO, master, mirror, isolated, true)
	if err != nil {
		log.Fatal(err)
	}

	w.targetsNum = graph.GetTargetsNum(targetsFile)

	loadTime := time.Since(start)
	fmt.Printf("loadGraph Time: %v", loadTime)

	if w.g == nil {
		log.Println("can't load graph")
	}
	//w.outerMsg = algorithm.GenerateOuterMsg(w.g.GetFOs())

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