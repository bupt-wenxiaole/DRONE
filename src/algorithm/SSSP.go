package algorithm

import (
	"container/heap"
	"graph"
	"time"
	//"fmt"
	"log"
)

// for more information about this implement of priority queue,
// you can reference https://golang.org/pkg/container/heap/
// we use Pair for store distance message associated with node ID

// in this struct, Distance is the distance from the global start node to this node
type Pair struct {
	NodeId   graph.ID
	Distance float64
}

type PriorityQueue []*Pair

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].Distance < pq[j].Distance
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*Pair))
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	p := old[n-1]
	*pq = old[0 : n-1]
	return p
}

// g is the graph structure of graph
// distance stores distance from startId to this nodeId, and is initialed to be infinite
// exchangeMsg stores distance from startId to this nodeId, where the nodeId belong to Fi.O, and is initialed to be infinite
// routeTable is created by func GenerateRouteTable
// startID is the start id of global graph, usually we set it to node 1
// returned bool value indicates which there has some message need to be send
// the map value is the message need to be send
// map[i] is a list of message need to be sent to partition i
func SSSP_PEVal(g graph.Graph, distance map[graph.ID]float64, startID graph.ID) (bool, map[int][]*Pair, float64, float64, int64, int32, int32) {
	log.Printf("start id:%v\n", startID.IntVal())
	nodes := g.GetNodes()
	// if this partition doesn't include startID, just return
	if _, ok := nodes[startID]; !ok {
		return false, make(map[int][]*Pair), 0, 0, 0, 0, 0
	}
	pq := make(PriorityQueue, 0)
	updated := make(map[graph.ID]bool)

	startPair := &Pair{
		NodeId:   startID,
		Distance: 0,
	}
	heap.Push(&pq, startPair)
	distance[startID] = 0

	var iterationNum int64 = 0
	itertationStartTime := time.Now()
	// begin SSSP iteration
	for pq.Len() > 0 {
		iterationNum++
		top := heap.Pop(&pq).(*Pair)
		srcID := top.NodeId
		nowDis := top.Distance
		if nowDis > distance[srcID] {
			continue
		}

		if !g.IsMaster(srcID) {
			updated[srcID] = true
		}

		targets, _ := g.GetTargets(srcID)
		for disID := range targets {
			weight, _ := g.GetWeight(srcID, disID)
			if distance[disID] > nowDis+weight {
				heap.Push(&pq, &Pair{NodeId: disID, Distance: nowDis + weight})
				distance[disID] = nowDis + weight
			}
		}
	}
	iterationTime := time.Since(itertationStartTime).Seconds()
	combineStartTime := time.Now()
	//end SSSP iteration
	messageMap := make(map[int][]*Pair)

	mirrors := g.GetMirrors()
	for id := range updated {
		partition := mirrors[id]
		dis := distance[id]
		if _, ok := messageMap[partition]; !ok {
			messageMap[partition] = make([]*Pair, 0)
		}
		messageMap[partition] = append(messageMap[partition], &Pair{NodeId: id, Distance: dis})
	}

	combineTime := time.Since(combineStartTime).Seconds()

	updatePairNum := int32(len(updated))
	dstPartitionNum := int32(len(messageMap))
	return len(messageMap) != 0, messageMap, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum
}

// the arguments is similar with PEVal
// the only difference is updated, which is the message this partition received
func SSSP_IncEval(g graph.Graph, distance map[graph.ID]float64, updated []*Pair, updateMaster map[graph.ID]bool, updateMirror map[graph.ID]bool, updatedByMessage map[graph.ID]bool) (bool, map[int][]*Pair, float64, float64, int64, int32, int32, float64, int32, int32) {
	if len(updated) == 0 && len(updatedByMessage) == 0 {
		return false, make(map[int][]*Pair), 0, 0, 0, 0, 0, 0, 0, 0
	}

	pq := make(PriorityQueue, 0)

	aggregatorOriSize := int32(len(updated))
	aggregateStart := time.Now()
	aggregateTime := time.Since(aggregateStart).Seconds()
	aggregatorReducedSize := int32(len(updated))

	for _, ssspMsg := range updated {
		//log.Printf("update message: id:%v, val:%v\n", ssspMsg.NodeId, ssspMsg.Distance)
		if ssspMsg.Distance < distance[ssspMsg.NodeId] {
			distance[ssspMsg.NodeId] = ssspMsg.Distance
			updatedByMessage[ssspMsg.NodeId] = true
		}
	}

	for id := range updatedByMessage {
		//log.Printf("updatedId: %v\n", id)

		dis := distance[id]
		startPair := &Pair{
			NodeId:   id,
			Distance: dis,
		}
		heap.Push(&pq, startPair)
	}

	var iterationNum int64 = 0
	iterationStartTime := time.Now()

	for pq.Len() > 0 {
		iterationNum++
		top := heap.Pop(&pq).(*Pair)
		srcID := top.NodeId
		nowDis := top.Distance
		if nowDis > distance[srcID] {
			continue
		}

		targets, _ := g.GetTargets(srcID)
		for disID := range targets {
			weight, _ := g.GetWeight(srcID, disID)
			if distance[disID] > nowDis+weight {
				heap.Push(&pq, &Pair{NodeId: disID, Distance: nowDis + weight})
				distance[disID] = nowDis + weight

				if !g.IsMaster(disID) {
					updateMirror[disID] = true
					//log.Printf("inceval update mirror id: %v\n", disID)
				} else {
					updateMaster[disID] = true
				}
			}
		}
	}
	iterationTime := time.Since(iterationStartTime).Seconds()
	combineStartTime := time.Now()

	messageMap := make(map[int][]*Pair)
	mirrors := g.GetMirrors()
	for id := range updateMirror {
		partition := mirrors[id]
		dis := distance[id]

		//log.Printf("nodeId: %v, Distance:%v\n", id, dis)
		if _, ok := messageMap[partition]; !ok {
			messageMap[partition] = make([]*Pair, 0)
		}
		messageMap[partition] = append(messageMap[partition], &Pair{NodeId: id, Distance: dis})
	}

	combineTime := time.Since(combineStartTime).Seconds()

	updatePairNum := int32(len(updateMirror))
	dstPartitionNum := int32(len(messageMap))

	//log.Printf("zs-log: messageMap:%v\n", messageMap)
	return len(messageMap) != 0 || len(updateMaster) != 0, messageMap, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, aggregateTime, aggregatorOriSize, aggregatorReducedSize
}
