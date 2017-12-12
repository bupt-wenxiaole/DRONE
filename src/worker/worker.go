package worker

import (
    pb "protobuf"
    "google.golang.org/grpc"
    "graph"
    "sync"
    "math"
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

    workers []string
    selfId int        // the id of this worker itself in workers
    grpcHandlers []*grpc.ClientConn

    g graph.Graph
    distance map[graph.ID]int64
    exchangeMsg map[graph.ID]int64
}

func (w *Worker) Lock() {
    w.mutex.Lock()
}

func (w * Worker) UnLock() {
    w.mutex.Unlock()
}

func newWorker(name string) *Worker {
    w := new(Worker)
    w.mutex = new(sync.Mutex)
    return w
}