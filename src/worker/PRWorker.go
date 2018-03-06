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

func (w *PRWorker) ShutDown(ctx context.Context, args *pb.ShutDownRequest) (*pb.ShutDownResponse, error) {
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
	nodeNum := algorithm.PageRank_PEVal(w.g, w.prVal, w.partitionNum)
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
	/*
			fullSendStart = time.Now()
			for partitionId, message := range messages {
				client := pb.NewWorkerClient(w.grpcHandlers[partitionId+1])
				encodeMessage := make([]*pb.SSSPMessageStruct, 0)
				eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{int32(partitionId), int32(len(message))}
				SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)
				for _, msg := range message {
					encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.IntVal(), Distance: msg.Distance})
					//log.Printf("nodeId:%v dis:%v \n", msg.NodeId.String(), msg.Distance)
				}
				log.Printf("send partition id:%v\n", partitionId)
				_, err := client.SSSPSend(context.Background(), &pb.SSSPMessageRequest{Pair: encodeMessage})
				if err != nil {
					log.Println("send error")
					log.Fatal(err)
				}
			}
			fullSendDuration = time.Since(fullSendStart).Seconds()
		}
	*/
	return &pb.PEvalResponse{Ok: true, Body: &pb.PEvalResponseBody{IterationNum: 0, IterationSeconds: 0,
		CombineSeconds: 0, UpdatePairNum: 0, DstPartitionNum: 0, AllPeerSend: 0, PairNum: nil}}, nil
}

func (w *PRWorker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	w.iterationNum++
	if w.iterationNum == 1 {
		w.totalVertexNum += int64(w.updated[graph.StringID(-1)])
		w.updated = make(map[graph.ID]float64, 0)
		log.Printf("total vertex num:%v", w.totalVertexNum)
	}

	isMessageToSend, messages := algorithm.PageRank_IncEval(w.g, w.prVal, w.oldPr, w.partitionNum, w.selfId-1, w.outerMsg, w.updated, w.totalVertexNum)
	w.updated = make(map[graph.ID]float64, 0)
	var fullSendStart time.Time
	var fullSendDuration float64
	var SlicePeerSend []*pb.WorkerCommunicationSize

	fullSendStart = time.Now()
	for partitionId, message := range messages {
		client := pb.NewWorkerClient(w.grpcHandlers[partitionId+1])
		encodeMessage := make([]*pb.PRMessageStruct, 0)
		eachWorkerCommunicationSize := &pb.WorkerCommunicationSize{int32(partitionId), int32(len(message))}
		SlicePeerSend = append(SlicePeerSend, eachWorkerCommunicationSize)
		for _, msg := range message {
			encodeMessage = append(encodeMessage, &pb.PRMessageStruct{NodeID: msg.ID.IntVal(), PrVal:msg.PRValue})
		}
		_, err := client.PRSend(context.Background(), &pb.PRMessageRequest{Pair: encodeMessage})
		if err != nil {
			log.Fatal(err)
		}
	}

	fullSendDuration = time.Since(fullSendStart).Seconds()

	return &pb.IncEvalResponse{Update: isMessageToSend, Body: &pb.IncEvalResponseBody{AggregatorOriSize: 0,
		AggregatorSeconds: 0, AggregatorReducedSize: 0, IterationSeconds: 0,
		CombineSeconds: 0, IterationNum: 0, UpdatePairNum: 0, DstPartitionNum: 0, AllPeerSend: fullSendDuration,
		PairNum: SlicePeerSend}}, nil
}

func (w *PRWorker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	fs := tools.GenerateAlluxioClient(tools.AlluxioHost)

	result := make([]string, 0)
	for id, pr := range w.prVal {
		result = append(result, id.String()+"\t"+strconv.FormatFloat(pr, 'E', -1, 64))
	}

	ok, err := tools.WriteToAlluxio(fs, tools.ResultPath+"result_"+strconv.Itoa(w.selfId), result)
	if err != nil {
		log.Panic(err)
	}

	return &pb.AssembleResponse{Ok: ok}, nil
}

func (w *PRWorker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
	return nil, nil
}
func (w *PRWorker) SimSend(ctx context.Context, args *pb.SimMessageRequest) (*pb.SimMessageResponse, error) {
	return nil, nil
}

func (w *PRWorker) PRSend(ctx context.Context, args *pb.PRMessageRequest) (*pb.PRMessageResponse, error) {
	log.Println("send receive")
	w.Lock()
	for _, msg := range args.Pair {
		w.updated[msg.NodeID] += msg.PrVal
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
	if tools.ReadFromTxt {
		graphIO, _ := os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "p/G." + strconv.Itoa(w.selfId-1))
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}

		fxiReader, _ := os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "p/F" + strconv.Itoa(w.selfId-1) + ".I")
		fxoReader, _ := os.Open(tools.NFSPath + strconv.Itoa(partitionNum) + "p/F" + strconv.Itoa(w.selfId-1) + ".O")
		defer fxiReader.Close()
		defer fxoReader.Close()

		w.g, err = graph.NewGraphFromTXT(graphIO, fxiReader, fxoReader, strconv.Itoa(w.selfId-1))
		if err != nil {
			log.Fatal(err)
		}
	} else {
		graphIO, _ := tools.ReadFromAlluxio(tools.GraphPath+"G"+suffix+strconv.Itoa(w.selfId-1)+".json", "G"+suffix+strconv.Itoa(w.selfId-1)+".json")
		defer tools.DeleteLocalFile("G" + suffix + strconv.Itoa(w.selfId-1) + ".json")
		defer graphIO.Close()

		if graphIO == nil {
			fmt.Println("graphIO is nil")
		}

		partitionIO, _ := tools.ReadFromAlluxio(tools.PartitionPath+"P"+suffix+strconv.Itoa(w.selfId-1)+".json", "P"+suffix+strconv.Itoa(w.selfId-1)+".json")
		defer tools.DeleteLocalFile("P" + suffix + strconv.Itoa(w.selfId-1) + ".json")
		defer partitionIO.Close()
		w.g, err = graph.NewGraphFromJSON(graphIO, partitionIO, strconv.Itoa(w.selfId-1))
		if err != nil {
			log.Fatal(err)
		}
	}

	loadTime := time.Since(start)
	fmt.Printf("loadGraph Time: %v", loadTime)

	if w.g == nil {
		log.Println("can't load graph")
	}
	w.outerMsg = algorithm.GenerateOuterMsg(w.g.GetFOs())
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
