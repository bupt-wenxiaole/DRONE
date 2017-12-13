package worker

import (
    pb "protobuf"
    "google.golang.org/grpc"
    "graph"
    "sync"
    "math"
    "context"
    "log"
    "algorithm"
    "net"
    "os"
    "bufio"
    "io"
    "strings"
    "strconv"
    "fmt"
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

    peers []string
    selfId int        // the id of this worker itself in workers
    grpcHandlers []*grpc.ClientConn

    g graph.Graph
    distance map[graph.ID]int64    //
    exchangeMsg map[graph.ID]int64
    updated []*algorithm.Pair

    routeTable map[graph.ID][]*algorithm.BoundMsg

    iterationNum int
    stopChannel chan bool
}

func (w *Worker) Lock() {
    w.mutex.Lock()
}

func (w *Worker) UnLock() {
    w.mutex.Unlock()
}

func (w *Worker) ShutDown(ctx context.Context, args *pb.ShutDownRequest ) (*pb.ShutDownResponse, error) {
    w.Lock()
    defer w.Lock()

    for _, handle := range w.grpcHandlers {
        handle.Close()
    }
    w.stopChannel <- true
}

func (w *Worker) PEval (ctx context.Context, args *pb.PEvalRequest) (*pb.PEvalResponse, error) {
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

    // TODO load graph from alluxio

    // Initial worker
    w.routeTable = algorithm.GenerateRouteTable(w.g.GetFOs())
    w.distance, w.exchangeMsg = Generate(w.g)

    isMessageToSend, messages := algorithm.SSSP_PEVal(w.g, w.distance, w.exchangeMsg, w.routeTable, graph.StringID("1"))
    if !isMessageToSend {
        return &pb.PEvalResponse{Ok:isMessageToSend}, nil
    } else {
        for partitionId, message := range messages {
            client := pb.NewWorkerClient(w.grpcHandlers[partitionId])
            encodeMessage := make([]*pb.SSSPMessageStruct, 0)
            for _, msg := range message {
                encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.String(), Distance:msg.Distance,})
            }
            _, err := client.Send(context.Background(), &pb.SSSPMessageRequest{Pair: encodeMessage})
            if err != nil {
                log.Fatal(err)
            }
        }
    }
    return &pb.PEvalResponse{Ok:isMessageToSend}, nil
}

func (w *Worker) IncEval (ctx context.Context, args *pb.IncEvalRequest) (*pb.IncEvalResponse, error) {
    w.updated = make([]*algorithm.Pair, 0)

    isMessageToSend, messages := algorithm.SSSP_IncEval(w.g, w.distance, w.exchangeMsg, w.routeTable, w.updated)
    if !isMessageToSend {
        return &pb.IncEvalResponse{Update:isMessageToSend}, nil
    } else {
        for partitionId, message := range messages {
            client := pb.NewWorkerClient(w.grpcHandlers[partitionId])
            encodeMessage := make([]*pb.SSSPMessageStruct, 0)
            for _, msg := range message {
                encodeMessage = append(encodeMessage, &pb.SSSPMessageStruct{NodeID: msg.NodeId.String(), Distance:msg.Distance,})
            }
            _, err := client.Send(context.Background(), &pb.SSSPMessageRequest{Pair: encodeMessage})
            if err != nil {
                log.Fatal(err)
            }
        }
    }
    return &pb.IncEvalResponse{Update:isMessageToSend}, nil
}

func (w *Worker) Assemble (ctx context.Context, args *pb.AssembleRequest) (*pb.AssembleResponse, error) {
    //TODO: write distance to alluxio
    return &pb.AssembleResponse{Ok:true}, nil
}

func (w *Worker) Send(ctx context.Context, args *pb.SSSPMessageRequest) (*pb.SSSPMessageResponse, error) {
    decodeMessage := make([]*algorithm.Pair, 0)

    for _, msg := range args.Pair {
        decodeMessage = append(decodeMessage, &algorithm.Pair{NodeId:graph.StringID(msg.NodeID), Distance:msg.Distance})
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
    return w
}

func run(id int) {
    w := newWorker(id)

    //read config file
    f, err := os.Open("../test_data/config.txt")
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()
    rd := bufio.NewReader(f)
    for {
        line, err := rd.ReadString('\n')
        line = strings.Split(line, "\n")[0]    //delete the end "\n"
        if err != nil || io.EOF == err {
            break
        }

        conf := strings.Split(line, ",")
        w.peers = append(w.peers, conf[1])
    }


    ln, err := net.Listen("tcp", ":" + strings.Split(w.peers[w.selfId], ":")[1])

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

    <- w.stopChannel
}