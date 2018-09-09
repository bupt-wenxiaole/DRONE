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
)

// rpc send has max size limit, so we spilt our transfer into many small block
func Peer2PeerCCSend(client pb.WorkerClient, message []*pb.SimMessageStruct, id int)  {

	for len(message) > tools.RPCSendSize {
		slice := message[0:tools.RPCSendSize]
		message = message[tools.RPCSendSize:]
		_, err := client.SimSend(context.Background(), &pb.SimMessageRequest{Pair: slice})
		if err != nil {
			log.Printf("send to %v error\n", id)
			log.Fatal(err)
		}
	}
	if len(message) != 0 {
		_, err := client.SimSend(context.Background(), &pb.SimMessageRequest{Pair: message})
		if err != nil {
			log.Printf("send to %v error\n", id)
			log.Fatal(err)
		}
	}
}

type CCWorker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	grpcHandlers map[int]*grpc.ClientConn
	workerNum int

	g           graph.Graph
	message     []*algorithm.CCPair

	CCValue     map[graph.ID]int64
	ExchangeValue map[graph.ID]int64

	iterationNum int
	stopChannel  chan bool
}

func (w *CCWorker) Lock() {
	w.mutex.Lock()
}

func (w *CCWorker) UnLock() {
	w.mutex.Unlock()
}

func (w *CCWorker) ShutDown(ctx context.Context, args *pb.ShutDownRequest) (*pb.ShutDownResponse, error) {
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

func (w * CCWorker) peval(args *pb.PEvalRequest, id int)  {
	var fullSendStart time.Time
	var fullSendDuration float64
	SlicePeerSend := make([]*pb.WorkerCommunicationSize, 0)


	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum := algorithm.CC_PEVal(w.g, w.CCValue, w.ExchangeValue)

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
		//messageSlice := make([])

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

				go func(partitionId int, message []*algorithm.CCPair) {
					defer wg.Done()
					workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
					if err != nil {
						log.Fatal(err)
					}
					defer workerHandle.Close()

					client := pb.NewWorkerClient(workerHandle)
					encodeMessage := make([]*pb.SimMessageStruct, 0)
					for _, msg := range message {
						encodeMessage = append(encodeMessage, &pb.SimMessageStruct{DataId:msg.NodeId.IntVal(), PatternId:msg.CCvalue})
					}
					Peer2PeerCCSend(client, encodeMessage, partitionId + 1)
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
}

func (w *CCWorker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
	go w.peval(args, w.selfId)
	return &pb.PEvalResponse{}, nil
}

func (w *CCWorker) incEval(args *pb.IncEvalRequest, id int) {
	w.iterationNum++
	isMessageToSend, messages, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum := algorithm.CC_IncEval(w.g, w.CCValue, w.ExchangeValue, w.message)
	w.message = make([]*algorithm.CCPair, 0)

	var fullSendStart time.Time
	var fullSendDuration float64
	SlicePeerSend := make([]*pb.WorkerCommunicationSize, 0)
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
		//messageSlice := make([])

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

				go func(partitionId int, message []*algorithm.CCPair) {
					defer wg.Done()
					workerHandle, err := grpc.Dial(w.peers[partitionId+1], grpc.WithInsecure())
					if err != nil {
						log.Fatal(err)
					}
					defer workerHandle.Close()

					client := pb.NewWorkerClient(workerHandle)
					encodeMessage := make([]*pb.SimMessageStruct, 0)
					for _, msg := range message {
						encodeMessage = append(encodeMessage, &pb.SimMessageStruct{DataId:msg.NodeId.IntVal(), PatternId:msg.CCvalue})
					}
					Peer2PeerCCSend(client, encodeMessage, partitionId + 1)
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
}

func (w *CCWorker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	go w.incEval(args, w.selfId)
	return &pb.IncEvalResponse{}, nil
}

func (w *CCWorker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	var f *os.File
	if tools.WorkerOnSC {
		f, _ = os.Create(tools.ResultPath + "ccresult_" + strconv.Itoa(w.selfId-1))
	} else {
		f, _ = os.Create(tools.ResultPath + "/result_" + strconv.Itoa(w.selfId-1))
	}
	writer := bufio.NewWriter(f)
	defer f.Close()

	for id, cc := range w.CCValue {
		writer.WriteString(id.String() + "\t" + strconv.FormatInt(cc, 10) + "\n")
	}
	writer.Flush()

	return &pb.AssembleResponse{Ok: true}, nil
}

func (w *CCWorker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
	return nil, nil
}

func (w *CCWorker) SimSend(ctx context.Context, args *pb.SimMessageRequest) (*pb.SimMessageResponse, error) {
	//log.Println("cc send receive")
	message := make([]*algorithm.CCPair, 0)
	for _, messagePair := range args.Pair {
		message = append(message, &algorithm.CCPair{NodeId: graph.StringID(messagePair.DataId), CCvalue: messagePair.PatternId})
	}

	w.Lock()
	w.message = append(w.message, message...)
	w.UnLock()

	return &pb.SimMessageResponse{}, nil
}
func (w *CCWorker) PRSend(ctx context.Context, args *pb.PRMessageRequest) (*pb.PRMessageResponse, error) {
	return nil, nil
}

func newCCWorker(id, partitionNum int) *CCWorker {
	w := new(CCWorker)
	w.mutex = new(sync.Mutex)
	w.selfId = id
	w.peers = make([]string, 0)
	w.message = make([]*algorithm.CCPair, 0)
	w.iterationNum = 0
	w.stopChannel = make(chan bool)
	w.grpcHandlers = make(map[int]*grpc.ClientConn)

	w.CCValue = make(map[graph.ID]int64)
	w.ExchangeValue = make(map[graph.ID]int64)

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
		//graphIO, _ := os.Open(tools.NFSPath + "G" + strconv.Itoa(partitionNum) + "_" + strconv.Itoa(w.selfId-1) + ".json")
		graphIO, _ := os.Open(tools.NFSPath)
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}

		//partitionIO, _ := os.Open(tools.NFSPath + "P" + strconv.Itoa(partitionNum) + "_" + strconv.Itoa(w.selfId-1) + ".json")
		partitionIO, _ := os.Open(tools.PartitionPath)
		defer partitionIO.Close()

		w.g, err = graph.NewGraphFromJSON(graphIO, partitionIO, strconv.Itoa(w.selfId-1))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var graphIO, fxiReader, fxoReader *os.File
		if tools.WorkerOnSC {
			graphIO, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/G." + strconv.Itoa(w.selfId-1))
		} else {
			graphIO, _ = os.Open(tools.NFSPath + "G." + strconv.Itoa(w.selfId-1))
		}
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}
		if tools.WorkerOnSC {
			fxiReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/F" + strconv.Itoa(w.selfId-1) + ".I")
			fxoReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/F" + strconv.Itoa(w.selfId-1) + ".O")
		} else {
			fxiReader, _ = os.Open(tools.NFSPath + "F" + strconv.Itoa(w.selfId-1) + ".I")
			fxoReader, _ = os.Open(tools.NFSPath + "F" + strconv.Itoa(w.selfId-1) + ".O")
		}
		defer fxiReader.Close()
		defer fxoReader.Close()

		w.g, err = graph.NewGraphFromTXT(graphIO, fxoReader)
		if err != nil {
			log.Fatal(err)
		}
	}

	loadTime := time.Since(start)
	fmt.Printf("loadGraph Time: %v", loadTime)
	log.Printf("graph size:%v\n", len(w.g.GetNodes()))

	if w.g == nil {
		log.Println("can't load graph")
	}

	return w
}

func RunCCWorker(id, partitionNum int) {
	w := newCCWorker(id, partitionNum)

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
