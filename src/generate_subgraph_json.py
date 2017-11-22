import json
with open("/home/xwen/GRAPE/test_data/small_metis_graph", "r") as r1 , open("/home/xwen/GRAPE/test_data/small_metis_graph.part.2", "r") as r2:
    #r1.next()
    r1_lines = r1.readlines()
    r1_lines = r1_lines[1:]
    for i in range(len(r1_lines)):
        r1_lines[i] = r1_lines[i].strip().split()
        r1_lines[i] = map(int, r1_lines[i])
    r2_lines = r2.readlines()
    r2_lines = map(int, r2_lines)
    assert len(r1_lines) == len(r2_lines) 
    partition_num = 2
    subGraphJsonMap = {}
    for i in range(partition_num):
        subGraph = "Graph" + str(i)
        subGraphFI = subGraph + "F.I"
        subGraphFO = subGraph + "F.O"
        subGraphJsonMap[subGraph] = {}
        subGraphJsonMap[subGraphFI] = {}
        subGraphJsonMap[subGraphFO] = {}
    for j in range(len(r1_lines)):
        nodeIndex = j + 1
        nodeSubGraph = "Graph" + str(r2_lines[j])
        nodeSubGraphFI = nodeSubGraph + "F.I"
        nodeSubGraphFO = nodeSubGraph + "F.O"
        subGraphJsonMap[nodeSubGraph][str(nodeIndex)] = {}
        subGraphJsonMap[nodeSubGraphFI][str(nodeIndex)] = {}
        for k in range(0, len(r1_lines[j]), 2):
            # K means another node in pair 
            if r2_lines[r1_lines[j][k] - 1] == r2_lines[j]:
                #this two nodes belongs to one subgraph
                subGraphJsonMap[nodeSubGraph][str(nodeIndex)][str(r1_lines[j][k])] = r1_lines[j][k+1]
            else:
                #this two nodes belongs to different subgraphs, Border
                #   first fills in Fi.I
                #   then  fills in Fi.O
                subGraphJsonMap[nodeSubGraphFI][str(nodeIndex)][str(r1_lines[j][k])] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])
                if str(r1_lines[j][k]) in subGraphJsonMap[nodeSubGraphFO]:
                    subGraphJsonMap[nodeSubGraphFO][str(r1_lines[j][k])][str(nodeIndex)] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])
                else:
                    subGraphJsonMap[nodeSubGraphFO][str(r1_lines[j][k])] = {}
                    subGraphJsonMap[nodeSubGraphFO][str(r1_lines[j][k])][str(nodeIndex)] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])

with open("/home/xwen/GRAPE/src/subgraph_json", "w") as w:
    w.write(json.dumps(subGraphJsonMap, sort_keys=True, indent=4, separators=(',', ': ')))
        




