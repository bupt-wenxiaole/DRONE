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

type updateMsg struct {
	Partition int
	Id graph.ID
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

func combine(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// this function is used for combine transfer message
func SSSP_aggregateMsg(oriMsg []*Pair) []*Pair {
	msg := make([]*Pair, 0)
	msgMap := make(map[graph.ID]float64)
	for _, m := range oriMsg {
		if beforeVal, ok := msgMap[m.NodeId]; ok {
			msgMap[m.NodeId] = combine(beforeVal, m.Distance)
		} else {
			msgMap[m.NodeId] = m.Distance
		}
	}

	for id, dis := range msgMap {
		msg = append(msg, &Pair{NodeId: id, Distance: dis})
	}
	return msg
}

// g is the graph structure of graph
// distance stores distance from startId to this nodeId, and is initialed to be infinite
// exchangeMsg stores distance from startId to this nodeId, where the nodeId belong to Fi.O, and is initialed to be infinite
// routeTable is created by func GenerateRouteTable
// startID is the start id of global graph, usually we set it to node 1
// returned bool value indicates which there has some message need to be send
// the map value is the message need to be send
// map[i] is a list of message need to be sent to partition i
func SSSP_PEVal(g graph.Graph, distance map[graph.ID]float64, exchangeMsg map[graph.ID]float64, startID graph.ID) (bool, map[int][]*Pair, float64, float64, int64, int32, int32) {
	log.Printf("start id:%v\n", startID.IntVal())
	itertationStartTime := time.Now()
	nodes := g.GetNodes()
	// if this partition doesn't include startID, just return
	if _, ok := nodes[startID]; !ok {
		return false, make(map[int][]*Pair), 0, 0, 0, 0, 0
	}
	pq := make(PriorityQueue, 0)
	route := g.GetRoute()
	updated := make(map[updateMsg]bool)

	startPair := &Pair{
		NodeId:   startID,
		Distance: 0,
	}
	heap.Push(&pq, startPair)

	var iterationNum int64 = 0
	// begin SSSP iteration
	for pq.Len() > 0 {
		iterationNum++
		top := heap.Pop(&pq).(*Pair)
		srcID := top.NodeId
		nowDis := top.Distance
		if nowDis >= distance[srcID] {
			continue
		}
		distance[srcID] = nowDis
		//every iteration, query the update on Fi.O
		if msgs, ok := route[srcID]; ok {
			for dstId, msg := range msgs {
				if exchangeMsg[dstId] <= nowDis+msg.RelatedWgt() {
					continue
				}
				exchangeMsg[dstId] = nowDis + msg.RelatedWgt()
				updated[updateMsg{Id:dstId,Partition:msg.RoutePartition()}] = true
			}
		}

		targets := g.GetTargets(srcID)
		for disID, weight := range targets {
			if distance[disID] > nowDis+weight {
				heap.Push(&pq, &Pair{NodeId: disID, Distance: nowDis + weight})
			}
		}
	}
	combineStartTime := time.Now()
	//end SSSP iteration
	messageMap := make(map[int][]*Pair)
	for msg := range updated {
		id := msg.Id
		partition := msg.Partition
		dis := exchangeMsg[id]
		if _, ok := messageMap[partition]; !ok {
			messageMap[partition] = make([]*Pair, 0)
		}
		messageMap[partition] = append(messageMap[partition], &Pair{NodeId: id, Distance: dis})
	}

	combineTime := time.Since(combineStartTime).Seconds()

	updatePairNum := int32(len(updated))
	dstPartitionNum := int32(len(messageMap))
	iterationTime := time.Since(itertationStartTime).Seconds()
	return len(messageMap) != 0, messageMap, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum
}

// the arguments is similar with PEVal
// the only difference is updated, which is the message this partition received
func SSSP_IncEval(g graph.Graph, distance map[graph.ID]float64, exchangeMsg map[graph.ID]float64, updated []*Pair) (bool, map[int][]*Pair, float64, float64, int64, int32, int32, float64, int32, int32) {
	iterationStartTime := time.Now()
	if len(updated) == 0 {
		return false, make(map[int][]*Pair), 0, 0, 0, 0, 0, 0, 0, 0
	}

	route := g.GetRoute()
	pq := make(PriorityQueue, 0)
	updatedMsg := make(map[updateMsg]bool)

	aggregatorOriSize := int32(len(updated))
	aggregateStart := time.Now()
	updated = SSSP_aggregateMsg(updated)
	aggregateTime := time.Since(aggregateStart).Seconds()
	aggregatorReducedSize := int32(len(updated))

	for _, ssspMsg := range updated {
		startPair := &Pair{
			NodeId:   ssspMsg.NodeId,
			Distance: ssspMsg.Distance,
		}
		heap.Push(&pq, startPair)
	}

	var iterationNum int64 = 0

	for pq.Len() > 0 {
		iterationNum++

		top := heap.Pop(&pq).(*Pair)
		srcID := top.NodeId
		nowDis := top.Distance

		if nowDis >= distance[srcID] {
			continue
		}

		distance[srcID] = nowDis
		if msgs, ok := route[srcID]; ok {
			for dstId, msg := range msgs {
				if exchangeMsg[dstId] <= nowDis+msg.RelatedWgt() {
					continue
				}
				exchangeMsg[dstId] = nowDis + msg.RelatedWgt()
				updatedMsg[updateMsg{Id:dstId,Partition:msg.RoutePartition()}] = true
			}
		}

		targets := g.GetTargets(srcID)
		for disID, weight := range targets {
			if distance[disID] > nowDis+weight {
				heap.Push(&pq, &Pair{NodeId: disID, Distance: nowDis + weight})
			}
		}
	}
	combineStartTime := time.Now()

	messageMap := make(map[int][]*Pair)
	for msg := range updatedMsg {

		partition := msg.Partition
		dis := exchangeMsg[msg.Id]
		if _, ok := messageMap[partition]; !ok {
			messageMap[partition] = make([]*Pair, 0)
		}
		messageMap[partition] = append(messageMap[partition], &Pair{NodeId: msg.Id, Distance: dis})
	}

	combineTime := time.Since(combineStartTime).Seconds()

	updatePairNum := int32(len(updatedMsg))
	dstPartitionNum := int32(len(messageMap))
	iterationTime := time.Since(iterationStartTime).Seconds()
	return len(messageMap) != 0, messageMap, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, aggregateTime, aggregatorOriSize, aggregatorReducedSize
}
