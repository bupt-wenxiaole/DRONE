package graph

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	//"fmt"
	"bufio"
	"log"
	"tools"
)

type RouteMsg interface {
	//RelatedId() ID
	RelatedWgt() float64
	RoutePartition() int
}

type routeMsg struct {
	//relatedId      ID
	relatedWgt     float64
	routePartition int
}
/*
func (r *routeMsg) RelatedId() ID {
	return r.relatedId
}
*/
func (r *routeMsg) RelatedWgt() float64 {
	return r.relatedWgt
	//return 0
}

func (r *routeMsg) RoutePartition() int {
	return r.routePartition
}

func resolveJsonMap(jsonMap map[string]map[string]string, dstInner bool) map[ID]map[ID]RouteMsg {
	ansMap := make(map[ID]map[ID]RouteMsg)
	if jsonMap == nil {
		return ansMap
	}


	for srcID, dstMsg := range jsonMap {

		//msgList := make([]RouteMsg, 0)
		intId, _ := strconv.Atoi(srcID)
		stringId := StringID(intId)

		if dstInner {
			for dstID, msg := range dstMsg {
				dstIDInt, _ := strconv.Atoi(dstID)
				dst := StringID(dstIDInt)
				if _, ok := ansMap[dst]; !ok {
					ansMap[dst] = make(map[ID]RouteMsg)
				}

				split := strings.Split(msg, " ")
				wgt, _ := strconv.ParseFloat(split[0], 64)
				nextHop, _ := strconv.Atoi(split[1])

				route := &routeMsg{relatedWgt: wgt, routePartition: nextHop}
				ansMap[dst][stringId] = route
			}
		} else {
			ansMap[stringId] = make(map[ID]RouteMsg)
			for dstID, msg := range dstMsg {
				dstIDInt, _ := strconv.Atoi(dstID)
				dst := StringID(dstIDInt)

				split := strings.Split(msg, " ")
				wgt, _ := strconv.ParseFloat(split[0], 64)
				nextHop, _ := strconv.Atoi(split[1])

				route := &routeMsg{relatedWgt: wgt, routePartition: nextHop}
				ansMap[stringId][dst] = route
			}
		}
	}
	return ansMap
}
func resolveTagMap(jsonMap map[string]map[string]string) map[ID]bool {
	ansMap := make(map[ID]bool)
	if jsonMap == nil {
		return ansMap
	}

	for id := range jsonMap {
		intId, _ := strconv.Atoi(id)
		stringId := StringID(intId)
		ansMap[stringId] = true
	}
	return ansMap
}

func LoadRouteMsgFromJson(rd io.Reader, graphId string) (map[ID]bool, map[ID]map[ID]RouteMsg, error) {
	dec := json.NewDecoder(rd)
	//          GraphXF.I/O    srcID      dstID   attr
	js := make(map[string]map[string]map[string]string)

	for {
		if err := dec.Decode(&js); err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
	}

	FOMap := js["Graph"+graphId+"F.O"]
	outerTag := resolveTagMap(FOMap)
	route := resolveJsonMap(FOMap, true)

	return outerTag, route, nil
}

// srcInner 为true 意味着src点属于graph内部点，反之意味着dst点是内点
func LoadRouteMsgFromTxt(rd io.Reader, srcInner bool, g Graph)(map[ID]map[ID]RouteMsg, error) {
	ansMap := make(map[ID]map[ID]RouteMsg)
	bufrd := bufio.NewReader(rd)

	for {
		line, err := bufrd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		paras := strings.Split(strings.Split(line, "\n")[0], " ")
		parseSrc, err := strconv.ParseInt(paras[0], 10, 64)
		if err != nil {
			log.Fatal("parse src node id error")
		}
		parseDst, err := strconv.ParseInt(paras[1], 10, 64)
		if err != nil {
			log.Fatal("parse dst node id error")
		}

		srcId := StringID(parseSrc)
		dstId := StringID(parseDst)

		if !srcInner {
			srcId = StringID(parseDst)
			dstId = StringID(parseSrc)
		}
		nd := g.GetNode(srcId)
		if nd == nil {
			intId := srcId.IntVal()
			nd = NewNode(intId, int64(intId%tools.GraphSimulationTypeModel))
			if ok := g.AddNode(nd); !ok {
				log.Fatal("add node error")
			}
		}

		/*
		weight, err := strconv.ParseFloat(paras[3], 64)
		if err != nil {
			log.Fatal("parse weight error")
		}*/
		weight := 0.0

		partition, err := strconv.Atoi(paras[2])
		if err != nil {
			log.Fatal("parse partition error")
		}

		if _, ok := ansMap[srcId]; !ok {
			ansMap[srcId] = make(map[ID]RouteMsg)
		}
		route := &routeMsg{relatedWgt: weight, routePartition: partition}
		ansMap[srcId][dstId] = route
	}
	return ansMap, nil
}

func LoadTagFromTxt(rd io.Reader)(map[ID]bool, error) {
	ansMap := make(map[ID]bool)
	bufrd := bufio.NewReader(rd)

	for {
		line, err := bufrd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		paras := strings.Split(strings.Split(line, "\n")[0], " ")
		parseDst, err := strconv.ParseInt(paras[1], 10, 64)
		if err != nil {
			log.Fatal("parse dst node id error")
		}
		dstId := StringID(parseDst)

		ansMap[dstId] = true
	}
	return ansMap, nil
}