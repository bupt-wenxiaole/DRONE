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
    "time"
    "os"
    pb "protobuf"
    "sync"
    "tools"
    "strconv"
    "strings"
    "golang.org/x/net/context"
	  "time"
	  "fmt"
)

//set up the alluxio client within minute, return a client point
func SetUpClient(host string) *alluxio.Client {
    fs := alluxio.NewClient(host, 39999, time.Minute)
    return fs
}


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

func (w *Worker) PEval(ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
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
	isMessageToSend, messages := algorithm.SSSP_PEVal(w.g, w.distance, w.exchangeMsg, w.routeTable, graph.StringID("1"))
	if !isMessageToSend {
		return &pb.PEvalResponse{Ok: isMessageToSend}, nil
	} else {
		for partitionId, message := range messages {
			client := pb.NewWorkerClient(w.grpcHandlers[partitionId + 1])
			encodeMessage := make([]*pb.SSSPMessageStruct, 0)
			for _, msg := range message {
				encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.String(), Distance: msg.Distance})
				//log.Printf("nodeId:%v dis:%v \n", msg.NodeId.String(), msg.Distance)
			}
			log.Printf("send partition id:%v\n", partitionId)
			_, err := client.SSSPSend(context.Background(), &pb.SSSPMessageRequest{Pair: encodeMessage})
			if err != nil {
				log.Println("send error")
				log.Fatal(err)
			}
		}
	}
	return &pb.PEvalResponse{Ok: isMessageToSend}, nil
}

func (w *Worker) IncEval(ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
	w.iterationNum++

	isMessageToSend, messages := algorithm.SSSP_IncEval(w.g, w.distance, w.exchangeMsg, w.routeTable, w.updated)
	w.updated = make([]*algorithm.Pair, 0)
	if !isMessageToSend {
		return &pb.IncEvalResponse{Update: isMessageToSend}, nil
	} else {
		for partitionId, message := range messages {
			client := pb.NewWorkerClient(w.grpcHandlers[partitionId + 1])
			encodeMessage := make([]*pb.SSSPMessageStruct, 0)
			for _, msg := range message {
				encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.String(), Distance: msg.Distance})
			}
			_, err := client.SSSPSend(context.Background(), &pb.SSSPMessageRequest{Pair: encodeMessage})
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

func (w *Worker) SSSPSend(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
	log.Println("send receive")
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

	start := time.Now()

	//graphIO, _ := os.Open("/home/xwen/GRAPE/src/G" + strconv.Itoa(w.selfId - 1) + ".json")
	graphIO, _ := os.Open("G" + strconv.Itoa(w.selfId - 1) + ".json")
	defer graphIO.Close()
	//partitionIO, _ := os.Open("/home/xwen/GRAPE/src/P" + strconv.Itoa(w.selfId - 1) + ".json")
	partitionIO, _ := os.Open("P" + strconv.Itoa(w.selfId - 1) + ".json")
	defer partitionIO.Close()
	w.g, err = graph.NewGraphFromJSON(graphIO, partitionIO, strconv.Itoa(w.selfId - 1))
	if err != nil {
		log.Fatal(err)
	}
	loadTime := time.Since(start)
	fmt.Printf("loadGraph Time: %vs", loadTime)

	if w.g == nil {
		log.Println("can't load graph")
	}
	// Initial some variables from graph
	w.routeTable = algorithm.GenerateRouteTable(w.g.GetFOs())
	w.distance, w.exchangeMsg = Generate(w.g)

	return w
}

func RunWorker(id int) {
	w := newWorker(id)
	
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
    response, err := registerClient.Register(context.Background(), &pb.RegisterRequest{WorkerIndex:int32(w.selfId)})
    if err != nil || !response.Ok {
        log.Fatal("error for register")
    }

    // wait for stop
	<-w.stopChannel
	log.Println("finish task")
}
