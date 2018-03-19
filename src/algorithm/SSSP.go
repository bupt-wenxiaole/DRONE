package algorithm

import (
	"container/heap"
	"graph"
	"time"
	//"fmt"
)

// for more information about this implement of priority queue,
// you can reference https://golang.org/pkg/container/heap/
// we use Pair for store distance message associated with node ID

// in this struct, Distance is the distance from the global start node to this node
type Pair struct {
	NodeId   graph.ID
	Distance int64
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

// store the weight of edge connected to DstID
// in this struct, routeLen is only the length of one edge
type BoundMsg struct {
	DstId    graph.ID
	RouteLen int64
}

// in the result -- routeTable, routeTable[i] stores a list of BoundMsg,
// routeTable[node_i][j].RouteLen is the weight of edge: node_i -> routeTable[node_i][j].DstID
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

func combine(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// this function is used for combine transfer message
func SSSP_aggregateMsg(oriMsg []*Pair) []*Pair {
	msg := make([]*Pair, 0)
	msgMap := make(map[graph.ID]int64)
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
func SSSP_PEVal(g graph.Graph, distance map[graph.ID]int64, exchangeMsg map[graph.ID]int64, routeTable map[graph.ID][]*BoundMsg, startID graph.ID) (bool, map[int][]*Pair, float64, float64, int64, int32, int32) {
	nodes := g.GetNodes()
	// if this partition doesn't include startID, just return
	if _, ok := nodes[startID]; !ok {
		return false, make(map[int][]*Pair), 0, 0, 0, 0, 0
	}
	FO := g.GetFOs()
	updatedID := make([]graph.ID, 0)
	pq := make(PriorityQueue, 0)

	startPair := &Pair{
		NodeId:   startID,
		Distance: 0,
	}
	heap.Push(&pq, startPair)

	var iterationNum int64 = 0
	itertationStartTime := time.Now()
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
		if msgs, ok := routeTable[srcID]; ok {
			for _, msg := range msgs {
				if exchangeMsg[msg.DstId] <= nowDis+msg.RouteLen {
					continue
				}
				exchangeMsg[msg.DstId] = nowDis + msg.RouteLen
				updatedID = append(updatedID, msg.DstId)
			}
		}

		targets, _ := g.GetTargets(srcID)
		for disID := range targets {
			l, _ := g.GetWeight(srcID, disID)
			weight := int64(l)
			if distance[disID] > nowDis+weight {
				heap.Push(&pq, &Pair{NodeId: disID, Distance: nowDis + weight})
			}
		}
	}
	iterationTime := time.Since(itertationStartTime).Seconds()
	combineStartTime := time.Now()
	//end SSSP iteration
	filterMap := make(map[graph.ID]bool)
	messageMap := make(map[int][]*Pair)
	for _, id := range updatedID {
		if _, ok := filterMap[id]; ok {
			continue
		}
		filterMap[id] = true
		//TODO: from the GPAPE paper, Json file implementation is a little weird, F.O's partition is only related with first level key
		partition := FO[id][0].RoutePartition()
		dis := exchangeMsg[id]
		if _, ok := messageMap[partition]; !ok {
			messageMap[partition] = make([]*Pair, 0)
		}
		messageMap[partition] = append(messageMap[partition], &Pair{NodeId: id, Distance: dis})
	}

	combineTime := time.Since(combineStartTime).Seconds()

	updatePairNum := int32(len(filterMap))
	dstPartitionNum := int32(len(messageMap))
	return len(messageMap) != 0, messageMap, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum
}

// the arguments is similar with PEVal
// the only difference is updated, which is the message this partition received
func SSSP_IncEval(g graph.Graph, distance map[graph.ID]int64, exchangeMsg map[graph.ID]int64, routeTable map[graph.ID][]*BoundMsg, updated []*Pair) (bool, map[int][]*Pair, float64, float64, int64, int32, int32, float64, int32, int32) {
	if len(updated) == 0 {
		return false, make(map[int][]*Pair), 0, 0, 0, 0, 0, 0, 0, 0
	}

	FO := g.GetFOs()

	updatedID := make([]graph.ID, 0)
	pq := make(PriorityQueue, 0)

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
	iterationStartTime := time.Now()

	for pq.Len() > 0 {
		iterationNum++

		top := heap.Pop(&pq).(*Pair)
		srcID := top.NodeId
		nowDis := top.Distance

		if nowDis >= distance[srcID] {
			continue
		}

		distance[srcID] = nowDis
		if msgs, ok := routeTable[srcID]; ok {
			for _, msg := range msgs {
				if exchangeMsg[msg.DstId] <= nowDis+msg.RouteLen {
					continue
				}
				exchangeMsg[msg.DstId] = nowDis + msg.RouteLen
				updatedID = append(updatedID, msg.DstId)
			}
		}

		targets, _ := g.GetTargets(srcID)
		for disID := range targets {
			l, _ := g.GetWeight(srcID, disID)
			weight := int64(l)
			if distance[disID] > nowDis+weight {
				heap.Push(&pq, &Pair{NodeId: disID, Distance: nowDis + weight})
			}
		}
	}
	iterationTime := time.Since(iterationStartTime).Seconds()
	combineStartTime := time.Now()

	filterMap := make(map[graph.ID]bool)
	messageMap := make(map[int][]*Pair)
	for _, id := range updatedID {
		if _, ok := filterMap[id]; ok {
			continue
		}
		filterMap[id] = true

		partition := FO[id][0].RoutePartition()
		dis := exchangeMsg[id]
		if _, ok := messageMap[partition]; !ok {
			messageMap[partition] = make([]*Pair, 0)
		}
		messageMap[partition] = append(messageMap[partition], &Pair{NodeId: id, Distance: dis})
	}

	combineTime := time.Since(combineStartTime).Seconds()

	updatePairNum := int32(len(filterMap))
	dstPartitionNum := int32(len(messageMap))
	return len(messageMap) != 0, messageMap, iterationTime, combineTime, iterationNum, updatePairNum, dstPartitionNum, aggregateTime, aggregatorOriSize, aggregatorReducedSize
}
