package worker

import (
    "log"
    "net"
    "time"
    pb "protobuf"
    "golang.org/x/net/context"
    "google.golang.org/grpc"
    "math/rand"
    "strconv"
    "graph"
)

type DistKV struct {
    Node string
    Distance int
}
type DistKVChanGRPC struct {
    peerMessageChan chan DistKV 
}

type BoundMsg struct {
    DstId graph.ID
    RouteLen int64
}

func (s *DistKVChanGRPC) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
    //quote: 
    //In Google : we require that Go programmers pass a Context parameter as the first argument 
    //to every function on the call path between incoming and outgoing requests. 
    resp := &pb.PutResponse{}
    resp.Header = &pb.ResponseHeader{}
    distance, err := strconv.Atoi(string(r.DistanceTosourceNode))
    if err != nil {
        resp.Header.Ok = true
        return resp, err
    }
    var messageBlock DistKV = DistKV{Node:string(r.NodeIndex), Distance: distance}
    s.peerMessageChan <- messageBlock
    resp.Header.Ok = true
    return resp, nil
}

func startServerGRPC(port string, peerNumber int) {
    ln, err := net.Listen("tcp", port)
    if err != nil {
        panic(err)
    }

    s := &DistKVChanGRPC{}
    s.peerMessageChan = make(chan DistKV, peerNumber)
    grpcServer := grpc.NewServer()
    pb.RegisterDistKVServer(grpcServer, s)
    go func() {
        if err := grpcServer.Serve(ln); err != nil {
            panic(err)
        }
    }()
}

func Stress(port, endpoint string, keys, vals [][]byte, connsN, clientsN int) {
    //connsN means number of tcp connection, clientsN means numbers of grpc client
    go startServerGRPC(port, 100)

    conns := make([]*grpc.ClientConn, connsN)
    for i := range conns {
        conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
        if err != nil {
            panic(err)
        }
        conns[i] = conn
    }
    clients := make([]pb.DistKVClient, clientsN)
    for i := range clients {
        clients[i] = pb.NewDistKVClient(conns[i%int(connsN)])
    }

    requests := make(chan *pb.PutRequest, len(keys))
    done, errChan := make(chan struct{}), make(chan error)

    for i := range clients {
        go func(i int, requests chan *pb.PutRequest) {
            var cnt = 0
            for r := range requests {
                if _, err := clients[i].Put(context.Background(), r); err != nil {
                    errChan <- err
                    return
                }
            }
            done <- struct{}{}
        }(i, requests)
    }

    st := time.Now()

    for i := range keys {
        r := &pb.PutRequest{
            NodeIndex:   keys[i],
            DistanceTosourceNode: vals[i],
        }
        requests <- r
    }

    close(requests)

    cn := 0
    for cn != len(clients) {
        select {
        case err := <-errChan:
            panic(err)
        case <-done:
            cn++
        }
    }
    close(done)
    close(errChan)

    tt := time.Since(st)
    size := len(keys)
    pt := tt / time.Duration(size)
    log.Printf("GRPC took %v for %d requests with %d client(s) (%v per each).\n", tt, size, clientsN, pt)
}

func GenerateRouteTable(FO map[graph.ID][]graph.RouteMsg) map[graph.ID][]*BoundMsg {
    routeTable := make(map[graph.ID][]*BoundMsg)
    for fo, msgs := range FO {
        for _, msg := range msgs {
            srcId := msg.RelatedId()
            if _, ok := routeTable[srcId]; !ok {
                routeTable[srcId] = make([]*BoundMsg, 0)
            }

            nowMsg := &BoundMsg{
                DstId:    fo,
                RouteLen: int64(msg.RelatedWgt()),
            }
            routeTable[srcId] = append(routeTable[srcId], nowMsg)
        }
    }
    return routeTable
}

func main() {
    var keys, vals [][]byte
    var pairNum = 3
    for i := 0; i < pairNum; i++ {
        var key = []byte(strconv.Itoa(rand.Intn(10)))
        var val = []byte(strconv.Itoa(rand.Intn(100)))
        keys = append(keys, key)
        vals = append(vals, val)
    }
    Stress(":9000", "10.2.152.24:9000",keys, vals, 1, 10)

}