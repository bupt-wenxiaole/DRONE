package algorithm

import (
	"graph"
	"math"
	"log"
)

const eps = 0.01

type PRMessage struct {
	PRValue float64
	ID graph.ID
}

func PageRank_PEVal(g graph.Graph, prVal map[int64]float64, workerNum int) (int64, map[int64]float64) {
	nodeNum := len(g.GetNodes())
	initVal := 1.0 / float64(nodeNum * workerNum)
	for id := range g.GetNodes() {
		prVal[id.IntVal()] = initVal
	}

	tempPr := make(map[int64]float64)
	loopTime := 0
	for {
		if loopTime == 0 {
			log.Printf("accuracy: %v\n", eps * initVal)
		}
		log.Printf("loop time:%v\n", loopTime)
		var maxerr float64 = 0

		updated := false
		still := 0.0
		loopTime++

		for id := range g.GetNodes() {
			targets, _ := g.GetTargets(id)
			if len(targets) == 0 {
				still += prVal[id.IntVal()]
			} else {
				num := float64(len(targets))
				for dstId := range targets {
					tempPr[dstId.IntVal()] += 0.85 * prVal[id.IntVal()] / num
				}
			}
		}
		still = 0.85 * still / float64(nodeNum) + 0.15 * initVal
		for id := range g.GetNodes() {
			tempPr[id.IntVal()] += still
			if math.Abs(tempPr[id.IntVal()] - prVal[id.IntVal()]) > eps * initVal {
				updated = true
			}
			maxerr = math.Max(maxerr, math.Abs(tempPr[id.IntVal()] - prVal[id.IntVal()]))
		}
		log.Printf("max error:%v\n", maxerr)

		if !updated {
			prVal = tempPr
			break
		}

		prVal = tempPr
		tempPr = make(map[int64]float64)
	}

	log.Printf("loop time:%v\n", loopTime)
	return int64(nodeNum), prVal
}

func GenerateOuterMsg(FO map[graph.ID][]graph.RouteMsg) map[int64][]int64 {
	outerMsg := make(map[int64][]int64)
	for fo, msgs := range FO {
		for _, msg := range msgs {
			srcId := msg.RelatedId()
			if _, ok := outerMsg[srcId.IntVal()]; !ok {
				outerMsg[srcId.IntVal()] = make([]int64, 0)
			}

			nowMsg := fo.IntVal()
			outerMsg[srcId.IntVal()] = append(outerMsg[srcId.IntVal()], nowMsg)
		}
	}
	return outerMsg
}

func PageRank_IncEval(g graph.Graph, prVal map[int64]float64, oldPr map[int64]float64, workerNum int, partitionId int, outerMsg map[int64][]int64, messages map[int64]float64, totalVertexNum int64) (bool, map[int][]*PRMessage, map[int64]float64, map[int64]float64) {
	maxerr := 0.0

	still := 0.0
	initVal := 1.0 / float64(totalVertexNum)
	updated := false



	for id, msg := range messages {
		if id != -1 {
			prVal[id] += msg
		} else {
			still += msg
		}
	}

	var sum float64 = 0
	for id := range g.GetNodes() {
		prVal[id.IntVal()] += still * 0.85
		if math.Abs(prVal[id.IntVal()] - oldPr[id.IntVal()]) > eps * initVal {
			updated = true
		}
		maxerr = math.Max(maxerr, math.Abs(prVal[id.IntVal()] - oldPr[id.IntVal()]))
		sum += prVal[id.IntVal()]
	}
	//log.Printf("total vertex num:%v\n", totalVertexNum)
	log.Printf("still receive:%v\n", still)
	log.Printf("max error:%v\n", maxerr)
	log.Printf("sum value: %v\n", sum)

	tempPr := make(map[int64]float64)
	still = 0

	for id := range g.GetNodes() {
		targets, _ := g.GetTargets(id)
		sonNum := len(targets) + len(outerMsg[id.IntVal()])
		tempPr[id.IntVal()] += 0.15 * initVal
		if sonNum == 0 {
			still += prVal[id.IntVal()] / float64(totalVertexNum)
		} else {
			val := prVal[id.IntVal()] / float64(sonNum)
			//log.Printf("val: %v\n", val)
			for target := range targets {
				tempPr[target.IntVal()] += 0.85 * val
			}
			for _, outer := range outerMsg[id.IntVal()] {
				/*if id.IntVal() == 944 {
					log.Printf("out node:%v\n", outer)
				}*/
				tempPr[outer] += 0.85 * val
			}
		}
	}
	for id := range g.GetNodes() {
		tempPr[id.IntVal()] += 0.85 * still
	}

	reduceMsg := make(map[int][]*PRMessage)

	for i := 0; i < workerNum; i++ {
		if i == partitionId {
			continue
		}
		reduceMsg[i] = make([]*PRMessage, 0)
		reduceMsg[i] = append(reduceMsg[i], &PRMessage{PRValue:still,ID:graph.StringID(-1)})
	}

	log.Printf("still send:%v\n", still)

	for fo, routeMsg := range g.GetFOs() {
		partition := routeMsg[0].RoutePartition()
		reduceMsg[partition] = append(reduceMsg[partition], &PRMessage{PRValue:prVal[fo.IntVal()],ID:fo})
	}

	return updated, reduceMsg, prVal, tempPr
}
