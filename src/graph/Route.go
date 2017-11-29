package graph

import (
	"io"
	"encoding/json"
	"strings"
	"strconv"
	//"fmt"
)

type RouteMsg interface {
	RelatedId() ID
	RelatedWgt() int
	RoutePartition() int
}

type routeMsg struct {
	relatedId ID
	relatedWgt int
	routePartition int
}

func (r *routeMsg) RelatedId() ID {
	return r.relatedId
}

func (r *routeMsg) RelatedWgt() int {
	return r.relatedWgt
}

func (r *routeMsg) RoutePartition() int {
	return r.routePartition
}

func resolveJsonMap(jsonMap map[string]map[string]string) map[ID][]RouteMsg {
//	fmt.Println("start resolve")
	ansMap := make(map[ID][]RouteMsg)
	if jsonMap == nil {
		return ansMap
	}

	//fmt.Println(len(jsonMap))

	for srcID, dstMsg := range jsonMap {
	//	fmt.Println("srcID:" + string(srcID))

		msgList := make([]RouteMsg, 0)

		for dstID, msg := range dstMsg {
			split := strings.Split(msg, " ")
			wgt, _ := strconv.Atoi(split[0])
			nextHop, _ := strconv.Atoi(split[1])

			route := &routeMsg{relatedId: StringID(dstID), relatedWgt: wgt, routePartition: nextHop}

			msgList = append(msgList, route)
		}

		ansMap[StringID(srcID)] = msgList
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

	FIMap := js["Graph" + graphId + "F.I"]
	graphFI := resolveJsonMap(FIMap)

	FOMap := js["Graph" + graphId + "F.O"]
	graphFO := resolveJsonMap(FOMap)

	return graphFI, graphFO, nil
}
