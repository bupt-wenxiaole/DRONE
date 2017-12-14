package worker

import (
    "algorithm"
    "bufio"
    "google.golang.org/grpc"
    "graph"
    "io"
    "log"
    "math"
    "net"
    "os"
    pb "protobuf"
    "sync"
    "tools"
    "strconv"
    "strings"
    "golang.org/x/net/context"
)


func Generate(g graph.Graph) (map[graph.ID]int64, map[graph.ID]int64) {
	distance := make(map[graph.ID]int64)
	exchangeMsg := make(map[graph.ID]int64)

	for id := range g.GetNodes() {
		distance[id] = math.MaxInt64
	}

	for id := range g.GetFOs() {
		exchangeMsg[id] = math.MaxInt64
	}
	return distance, exchangeMsg
}

type Worker struct {
	mutex *sync.Mutex

	peers        []string
	selfId       int // the id of this worker itself in workers
	grpcHandlers []*grpc.ClientConn

	g           graph.Graph
	distance    map[graph.ID]int64 //
	exchangeMsg map[graph.ID]int64
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
	w.Lock()
	defer w.Lock()

	for _, handle := range w.grpcHandlers {
		handle.Close()
	}
	w.stopChannel <- true
	return &pb.ShutDownResponse{IterationNum: int32(w.iterationNum)}, nil
}

func (w *Worker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
	// init grpc handler and store it
	// cause until now, we can ensure all workers have in work,
	// so we do this here
	w.grpcHandlers = make([]*grpc.ClientConn, len(w.peers))
	for id, peer := range w.peers {
		if id == w.selfId {
			continue
		}
		conn, err := grpc.Dial(peer, grpc.WithInsecure())
		if err != nil {
			panic(err)
		}
		w.grpcHandlers[id] = conn
	}

	// Load graph data
	fs := tools.GenerateAlluxioClient(tools.AlluxioHost)
	graphIO, _ := tools.ReadFromAlluxio(fs, tools.GraphPath)
	defer graphIO.Close()
	partitionIO, _ := tools.ReadFromAlluxio(fs, tools.PartitionPath)
	defer partitionIO.Close()
	w.g, _ = graph.NewGraphFromJSON(graphIO, partitionIO, strconv.Itoa(w.selfId))

	// Initial some variables from graph
	w.routeTable = algorithm.GenerateRouteTable(w.g.GetFOs())
	w.distance, w.exchangeMsg = Generate(w.g)

	isMessageToSend, messages := algorithm.SSSP_PEVal(w.g, w.distance, w.exchangeMsg, w.routeTable, graph.StringID("1"))
	if !isMessageToSend {
		return &pb.PEvalResponse{Ok: isMessageToSend}, nil
	} else {
		for partitionId, message := range messages {
			client := pb.NewWorkerClient(w.grpcHandlers[partitionId])
			encodeMessage := make([]*pb.SSSPMessageStruct, 0)
			for _, msg := range message {
				encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.String(), Distance: msg.Distance})
			}
			_, err := client.Send(context.Background(), &pb.SSSPMessageRequest{Pair: encodeMessage})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	return &pb.PEvalResponse{Ok: isMessageToSend}, nil
}

func (w *Worker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	w.updated = make([]*algorithm.Pair, 0)
	w.iterationNum++

	isMessageToSend, messages := algorithm.SSSP_IncEval(w.g, w.distance, w.exchangeMsg, w.routeTable, w.updated)
	if !isMessageToSend {
		return &pb.IncEvalResponse{Update: isMessageToSend}, nil
	} else {
		for partitionId, message := range messages {
			client := pb.NewWorkerClient(w.grpcHandlers[partitionId])
			encodeMessage := make([]*pb.SSSPMessageStruct, 0)
			for _, msg := range message {
				encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.String(), Distance: msg.Distance})
			}
			_, err := client.Send(context.Background(), &pb.SSSPMessageRequest{Pair: encodeMessage})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	return &pb.IncEvalResponse{Update: isMessageToSend}, nil
}

func (w *Worker) Assemble(ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
	fs := tools.GenerateAlluxioClient(tools.AlluxioHost)

	result := make([]string, 0)
	for id, dist := range w.distance {
		result = append(result, id.String()+"\t"+strconv.FormatInt(dist, 10))
	}

	ok, err := tools.WriteToAlluxio(fs, tools.ResultPath+"result_"+strconv.Itoa(w.selfId), result)
	if err != nil {
		log.Panic(err)
	}

	return &pb.AssembleResponse{Ok: ok}, nil
}

func (w *Worker) Send(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
	decodeMessage := make([]*algorithm.Pair, 0)

	for _, msg := range args.Pair {
		decodeMessage = append(decodeMessage, &algorithm.Pair{NodeId: graph.StringID(msg.NodeID), Distance: msg.Distance})
	}

	w.Lock()
	w.updated = append(w.updated, decodeMessage...)
	w.UnLock()

	return &pb.SSSPMessageResponse{}, nil
}

func newWorker(id int) *Worker {
	w := new(Worker)
	w.mutex = new(sync.Mutex)
	w.selfId = id
	w.peers = make([]string, 0)
	w.updated = make([]*algorithm.Pair, 0)
	w.iterationNum = 0
	w.stopChannel = make(chan bool)

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

	return w
}

func RunWorker(id int) {
	w := newWorker(id)

	ln, err := net.Listen("tcp", ":"+strings.Split(w.peers[w.selfId], ":")[1])
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServer(grpcServer, w)
	go func() {
		if err := grpcServer.Serve(ln); err != nil {
			panic(err)
		}
	}()

    masterHandle, err := grpc.Dial(w.peers[0], grpc.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    registerClient := pb.NewMasterClient(masterHandle)
    response, err := registerClient.Register(context.Background(), &pb.RegisterRequest{WorkerIndex:int32(w.selfId)})
    if err != nil || !response.Ok {
        log.Fatal("error for register")
    }

    // wait for stop
	<-w.stopChannel
}
