package graph

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	//"fmt"
	"bufio"
	"log"
	"image/draw"
)

type RouteMsg interface {
	RelatedId() ID
	RelatedWgt() float64
	RoutePartition() int
}

type routeMsg struct {
	relatedId      ID
	relatedWgt     float64
	routePartition int
}

func (r *routeMsg) RelatedId() ID {
	return r.relatedId
}

func (r *routeMsg) RelatedWgt() float64 {
	return r.relatedWgt
}

func (r *routeMsg) RoutePartition() int {
	return r.routePartition
}

func resolveJsonMap(jsonMap map[string]map[string]string) map[ID][]RouteMsg {
	ansMap := make(map[ID][]RouteMsg)
	if jsonMap == nil {
		return ansMap
	}


	for srcID, dstMsg := range jsonMap {

		msgList := make([]RouteMsg, 0)

		for dstID, msg := range dstMsg {
			split := strings.Split(msg, " ")
			wgt, _ := strconv.ParseFloat(split[0], 64)
			nextHop, _ := strconv.Atoi(split[1])

			dstIDInt, _ := strconv.Atoi(dstID)

			route := &routeMsg{relatedId: StringID(dstIDInt), relatedWgt: wgt, routePartition: nextHop}

			msgList = append(msgList, route)
		}

		srcIdInt, _ := strconv.Atoi(srcID)
		ansMap[StringID(srcIdInt)] = msgList
	}
	return ansMap
}

func LoadRouteMsgFromJson(rd io.Reader, graphId string) (map[ID][]RouteMsg, map[ID][]RouteMsg, error) {
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

	FIMap := js["Graph"+graphId+"F.I"]
	graphFI := resolveJsonMap(FIMap)

	FOMap := js["Graph"+graphId+"F.O"]
	graphFO := resolveJsonMap(FOMap)

	return graphFI, graphFO, nil
}

func LoadRouteMsgFromTxt(rd io.Reader)(map[ID][]RouteMsg, error) {
	ansMap := make(map[ID][]RouteMsg)
	bufrd := bufio.NewReader(rd)
	for {
		line, err := bufrd.ReadString('\n')
		if err != nil || io.EOF == err {
			break
		}

		paras := strings.Split(line, " ")
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

		weight, err := strconv.ParseFloat(paras[3], 64)
		if err != nil {
			log.Fatal("parse weight error")
		}

		partition, err := strconv.Atoi(paras[2])
		if err != nil {
			log.Fatal("parse partition error")
		}

		if _, ok := ansMap[dstId]; !ok {
			ansMap[dstId] = make([]RouteMsg, 0)
		}
		route := &routeMsg{relatedId: srcId, relatedWgt: weight, routePartition: partition}
		ansMap[dstId] = append(ansMap[dstId], route)
	}

	return ansMap, nil
}