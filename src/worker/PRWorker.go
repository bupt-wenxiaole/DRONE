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
)

type PRWorker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	grpcHandlers []*grpc.ClientConn

	g            graph.Graph
	prVal        map[int64]float64
	oldPr        map[int64]float64
	partitionNum int
	totalVertexNum int64
	updated      map[int64]float64
	receiveBuffer map[int64]float64
	outerMsg    map[int64][]int64

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
func Peer2PeerPRSend(client pb.WorkerClient, message []*pb.PRMessageStruct, wg *sync.WaitGroup)  {

	for len(message) > tools.RPCSendSize {
		slice := message[0:tools.RPCSendSize]
		message = message[tools.RPCSendSize:]
		_, err := client.PRSend(context.Background(), &pb.PRMessageRequest{Pair: slice})
		if err != nil {
			log.Println("send error")
			log.Fatal(err)
		}
	}
	if len(message) != 0 {
		_, err := client.PRSend(context.Background(), &pb.PRMessageRequest{Pair: message})
		if err != nil {
			log.Println("send error")
			log.Fatal(err)
		}
	}
	wg.Done()
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

func (w *PRWorker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
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
	//var fullSendStart time.Time
	//var fullSendDuration float64
	//var SlicePeerSend []*pb.WorkerCommunicationSize
	var nodeNum int64
	nodeNum, w.prVal = algorithm.PageRank_PEVal(w.g, w.prVal, w.partitionNum)

	/*for id, val := range w.prVal {
		log.Printf("PEVal id:%v prval:%v\n", id, val)
	}*/

	w.totalVertexNum = nodeNum
	for partitionId := 1; partitionId <= w.partitionNum; partitionId++ {
		if partitionId == w.selfId {
			continue
		}
		encodeMessage := make([]*pb.PRMessageStruct, 0)
		encodeMessage = append(encodeMessage, &pb.PRMessageStruct{NodeID:-1,PrVal:float64(nodeNum)})
		client := pb.NewWorkerClient(w.grpcHandlers[partitionId])
		_, err := client.PRSend(context.Background(), &pb.PRMessageRequest{Pair: encodeMessage})
		if err != nil {
			log.Println("send error")
			log.Fatal(err)
		}
	}
	return &pb.PEvalResponse{Ok: true}, nil
}

func (w *PRWorker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	w.Lock()
	w.updated = w.receiveBuffer
	w.receiveBuffer = make(map[int64]float64, 0)
	w.UnLock()
/*
	w.iterationNum++
	if w.iterationNum == 1 {
		w.totalVertexNum += int64(w.updated[-1])
		w.updated = make(map[int64]float64, 0)
	}

	var isMessageToSend bool
	var messages map[int][]*algorithm.PRMessage

	runStart := time.Now()
	isMessageToSend, messages, w.oldPr, w.prVal = algorithm.PageRank_IncEval(w.g, w.prVal, w.oldPr, w.partitionNum, w.selfId-1, w.outerMsg, w.updated, w.totalVertexNum)
	runTime := time.Since(runStart).Seconds()

	w.updated = make(map[int64]float64, 0)
	var fullSendStart time.Time
	//var fullSendDuration float64
	var SlicePeerSend []*pb.WorkerCommunicationSize

	//time.Sleep(time.Second)

	var wg sync.WaitGroup
	fullSendStart = time.Now()
	for partitionId, message := range messages {
		//log.Printf("message send partition id:%v\n", partitionId)
		client := pb.NewWorkerClient(w.grpcHandlers[partitionId+1])
		encodeMessage := make([]*pb.PRMessageStruct, 0)
		eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{int32(partitionId), int32(len(message))}
		SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)
		for _, msg := range message {
			encodeMessage = append(encodeMessage, &pb.PRMessageStruct{NodeID: msg.ID.IntVal(), PrVal:msg.PRValue})
			//log.Printf("send id:%v prVal:%v\n", msg.ID.IntVal(), msg.PRValue)
		}
		wg.Add(1)
		go Peer2PeerPRSend(client, encodeMessage, &wg)
	}
	wg.Wait()
	//fullSendDuration = time.Since(fullSendStart).Seconds()
*/
	return &pb.IncEvalResponse{}, nil
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
	w.prVal = make(map[int64]float64, 0)
	w.oldPr = make(map[int64]float64, 0)
	w.partitionNum = partitionNum
	w.updated = make(map[int64]float64, 0)
	w.receiveBuffer = make(map[int64]float64, 0)

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
		defer partitionIO.Close()

		w.g, err = graph.NewGraphFromJSON(graphIO, partitionIO, strconv.Itoa(w.selfId-1))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		var graphIO, fxiReader, fxoReader *os.File
		if tools.WorkerOnSC {
			//graphIO, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "cores/G." + strconv.Itoa(w.selfId-1))
			graphIO, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "/G." + strconv.Itoa(w.selfId-1))
		} else {
			graphIO, _ = os.Open(tools.NFSPath + "G." + strconv.Itoa(w.selfId-1))
		}
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}
		if tools.WorkerOnSC {
			//fxiReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "cores/F" + strconv.Itoa(w.selfId-1) + ".I")
			//fxoReader, _ = os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "cores/F" + strconv.Itoa(w.selfId-1) + ".O")
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
