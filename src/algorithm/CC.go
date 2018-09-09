package algorithm

import (
	"graph"
	"sort"
	"time"
	"log"
)

type CCPair struct {
	NodeId graph.ID
	CCvalue  int64
}

type Array []*CCPair

func (a Array) Len() int { return len(a) }

func (a Array) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return a[i].CCvalue < a[j].CCvalue
}

func (a Array) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func dfs(s graph.ID, cc int64, g graph.Graph, ccValue map[graph.ID]int64, exchangeValue map[graph.ID]int64, updated map[updateMsg]bool) {
	for v := range g.GetTargets(s) {
		if ccValue[v] <= cc {
			continue
		}
		ccValue[v] = cc
		dfs(v, cc, g, ccValue, exchangeValue, updated)
	}
	for v, msg := range g.GetRoute()[s] {
		if exchangeValue[v] <= cc {
			continue
		}
		exchangeValue[v] = cc
		updated[updateMsg{Id:v, Partition:msg.RoutePartition()}] = true
	}
}

func CC_PEVal(g graph.Graph, ccValue map[graph.ID]int64, exchangeValue map[graph.ID]int64) (bool, map[int][]*CCPair, float64, float64, int64, int32, int32) {
	var array Array

	for v := range g.GetNodes() {
		ccValue[v] = v.IntVal()
		array = append(array, &CCPair{NodeId:v, CCvalue:v.IntVal()})
	}

	for _, msg := range g.GetRoute() {
		for v := range msg {
			exchangeValue[v] = v.IntVal()
		}
	}

	sort.Sort(array)

	updated := make(map[updateMsg]bool)
	itertationStartTime := time.Now()
	for _, pair := range array {
		v := pair.NodeId
		cc := pair.CCvalue
		if cc != ccValue[v] {
			continue
		}
		dfs(v, cc, g, ccValue, exchangeValue, updated)
	}
	iterationTime := time.Since(itertationStartTime).Seconds()

	combineStartTime := time.Now()
	messageMap := make(map[int][]*CCPair)
	for msg := range updated {
		partitonId := msg.Partition
		v := msg.Id

		if messageMap[partitonId] == nil {
			messageMap[partitonId] = make([]*CCPair, 0)
		}

		messageMap[partitonId] = append(messageMap[partitonId], &CCPair{NodeId:v, CCvalue:exchangeValue[v]})
	}
	combineTime := time.Since(combineStartTime).Seconds()

	updatePairNum := int32(len(updated))
	dstPartitionNum := int32(len(messageMap))
	return len(messageMap) != 0, messageMap, iterationTime, combineTime, 0, updatePairNum, dstPartitionNum
}

func CC_IncEval(g graph.Graph, ccValue map[graph.ID]int64, exchangeValue map[graph.ID]int64, messages []*CCPair) (bool, map[int][]*CCPair, float64, float64, int64, int32, int32) {

	sort.Sort(Array(messages))

	for i := 0; i < len(messages) - 1; i++ {
		if messages[i].CCvalue > messages[i + 1].CCvalue {
			log.Fatal("doesn't sort!")
		}
	}

	updated := make(map[updateMsg]bool)
	itertationStartTime := time.Now()
	for _, pair := range messages {
		v := pair.NodeId
		cc := pair.CCvalue
		if cc != ccValue[v] {
			continue
		}
		dfs(v, cc, g, ccValue, exchangeValue, updated)
	}
	iterationTime := time.Since(itertationStartTime).Seconds()

	combineStartTime := time.Now()
	messageMap := make(map[int][]*CCPair)
	for msg := range updated {
		partitonId := msg.Partition
		v := msg.Id

		if messageMap[partitonId] == nil {
			messageMap[partitonId] = make([]*CCPair, 0)
		}

		messageMap[partitonId] = append(messageMap[partitonId], &CCPair{NodeId:v, CCvalue:exchangeValue[v]})
	}
	combineTime := time.Since(combineStartTime).Seconds()

	updatePairNum := int32(len(updated))
	dstPartitionNum := int32(len(messageMap))
	return len(messageMap) != 0, messageMap, iterationTime, combineTime, 0, updatePairNum, dstPartitionNum
}

