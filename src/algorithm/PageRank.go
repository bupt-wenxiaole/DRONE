package algorithm

import (
	"graph"
	"math"
)

const eps = 0.01

type PRMessage struct {
	PRValue float64
	ID graph.ID
}

func PageRank_PEVal(g graph.Graph, prVal map[graph.ID]float64, workerNum int) int64 {
	nodeNum := len(g.GetNodes())
	initVal := 1.0 / float64(nodeNum * workerNum)
	for id := range g.GetNodes() {
		prVal[id] = initVal
	}

	tempPr := make(map[graph.ID]float64)
	for {
		updated := false
		still := 0.0

		for id := range g.GetNodes() {
			targets, _ := g.GetTargets(id)
			if len(targets) == 0 {
				still += prVal[id]
			} else {
				num := float64(len(targets))
				for dstId := range targets {
					tempPr[dstId] += 0.85 * prVal[id] / num
				}
			}
		}
		still = 0.85 * still / float64(nodeNum) + 0.15 * initVal
		for id := range g.GetNodes() {
			tempPr[id] += still
			if math.Abs(tempPr[id] - prVal[id]) > eps * initVal {
				updated = true
			}
		}

		if !updated {
			prVal = tempPr
			break
		}

		prVal = tempPr
		tempPr = make(map[graph.ID]float64)
	}
	return int64(nodeNum)
}

func GenerateOuterMsg(FO map[graph.ID][]graph.RouteMsg) map[graph.ID][]*graph.ID {
	outerMsg := make(map[graph.ID][]*graph.ID)
	for fo, msgs := range FO {
		for _, msg := range msgs {
			srcId := msg.RelatedId()
			if _, ok := outerMsg[srcId]; !ok {
				outerMsg[srcId] = make([]*graph.ID, 0)
			}

			nowMsg := &fo
			outerMsg[srcId] = append(outerMsg[srcId], nowMsg)
		}
	}
	return outerMsg
}

func PageRank_IncEval(g graph.Graph, prVal map[graph.ID]float64, oldPr map[graph.ID]float64, workerNum int, partitionId int, outerMsg map[graph.ID][]*graph.ID, messages map[graph.ID]float64, totalVertexNum int64) (bool, map[int][]*PRMessage) {
	still := 0.0
	initVal := 1.0 / float64(totalVertexNum)
	updated := false
	for id, msg := range messages {
		if id.IntVal() != -1 {
			prVal[id] += msg * 0.85
		} else {
			still += msg
		}
	}
	for id := range g.GetNodes() {
		prVal[id] += still * 0.85
		if math.Abs(prVal[id] - oldPr[id]) > eps * initVal {
			updated = true
		}
	}

	tempPr := make(map[graph.ID]float64)
	still = 0

	for id := range g.GetNodes() {
		targets, _ := g.GetTargets(id)
		sonNum := len(targets) + len(outerMsg[id])
		tempPr[id] += 0.15 * initVal
		if sonNum == 0 {
			still += prVal[id] / float64(totalVertexNum)
		} else {
			val := prVal[id] / float64(sonNum)
			for target := range targets {
				tempPr[target] += 0.85 * val
			}
			for _, outer := range outerMsg[id] {
				tempPr[*outer] += 0.85 * val
			}
		}
	}
	for id := range g.GetNodes() {
		tempPr[id] += 0.85 * still
	}

	reduceMsg := make(map[int][]*PRMessage)

	for i := 0; i < workerNum; i++ {
		if i == partitionId {
			continue
		}
		reduceMsg[i] = make([]*PRMessage, 0)
		reduceMsg[i] = append(reduceMsg[i], &PRMessage{PRValue:still,ID:graph.StringID(-1)})
	}

	for fo, routeMsg := range g.GetFOs() {
		partition := routeMsg[0].RoutePartition()
		reduceMsg[partition] = append(reduceMsg[partition], &PRMessage{PRValue:prVal[fo],ID:fo})
	}

	oldPr = prVal
	prVal = tempPr

	return updated, reduceMsg
}
