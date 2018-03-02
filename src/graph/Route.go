package graph

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	//"fmt"
	"bufio"
)

type RouteMsg interface {
	RelatedId() ID
	RelatedWgt() int
	RoutePartition() int
}

type routeMsg struct {
	relatedId      ID
	relatedWgt     int
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
	ansMap := make(map[ID][]RouteMsg)
	if jsonMap == nil {
		return ansMap
	}


	for srcID, dstMsg := range jsonMap {

		msgList := make([]RouteMsg, 0)

		for dstID, msg := range dstMsg {
			split := strings.Split(msg, " ")
			wgt, _ := strconv.Atoi(split[0])
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

func LoadRouteMsgFxIFromTxt(rd io.Reader)(map[ID][]RouteMsg, error) {
	stod := make(map[string]map[string]string)
	bufrd := bufio.NewReader(rd)
	for {
		line, err := bufrd.ReadString('\n')
		linelem := strings.Split(line, "\t")
		if err != nil {
			if err == io.EOF {
				if stod[linelem[0]] == nil {
					stod[linelem[0]] = make(map[string]string)
				}
				stod[linelem[0]][linelem[2]] = linelem[4]
				break
			}
			return nil, err
		}
		if stod[linelem[0]] == nil {
			stod[linelem[0]] = make(map[string]string)
		}
		stod[linelem[0]][linelem[2]] = linelem[4]
	}
	graphFI := resolveJsonMap(stod)

	return graphFI, nil
}

func LoadRouteMsgFxOFromTxt(rd io.Reader)(map[ID][]RouteMsg, error) {
	stod := make(map[string]map[string]string)
	bufrd := bufio.NewReader(rd)
	for {
		line, err := bufrd.ReadString('\n')
		linelem := strings.Split(line, "\t")
		if err != nil {
			if err == io.EOF {
				if stod[linelem[0]] == nil {
					stod[linelem[0]] = make(map[string]string)
				}
				stod[linelem[0]][linelem[2]] = linelem[4]
				break
			}
			return nil, err
		}
		if stod[linelem[0]] == nil {
			stod[linelem[0]] = make(map[string]string)
		}
		stod[linelem[0]][linelem[2]] = linelem[4]
	}
	graphFO := resolveJsonMap(stod)

	return graphFO, nil
}
