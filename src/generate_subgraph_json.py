import json
import sys
import getopt

def generate_json(Graph, MetisPartion, SubGraphJson, PartitionJson):
    with open(Graph, "r") as r1 , open(MetisPartion, "r") as r2:
        r1_lines = r1.readlines()
        r1_lines = r1_lines[1:]
        for i in range(len(r1_lines)):
            r1_lines[i] = r1_lines[i].strip().split()
            r1_lines[i] = map(int, r1_lines[i])
        r2_lines = r2.readlines()
        r2_lines = map(int, r2_lines)
        assert len(r1_lines) == len(r2_lines) 
        print MetisPartion  
        partition_num = int(MetisPartion[-1])
        print partition_num
        subGraphJsonMap = {}
	partitionJsonMap = {}
        for i in range(partition_num):
            subGraph = "Graph" + str(i)
            subGraphFI = subGraph + "F.I"
            subGraphFO = subGraph + "F.O"
            subGraphJsonMap[subGraph] = {}
            partitionJsonMap[subGraphFI] = {}
            partitionJsonMap[subGraphFO] = {}
        for j in range(len(r1_lines)):
            nodeIndex = j + 1
            nodeSubGraph = "Graph" + str(r2_lines[j])
            nodeSubGraphFI = nodeSubGraph + "F.I"
            nodeSubGraphFO = nodeSubGraph + "F.O"
            subGraphJsonMap[nodeSubGraph][str(nodeIndex)] = {}
            for k in range(0, len(r1_lines[j]), 2):
                # K means another node in pair 
                if r2_lines[r1_lines[j][k] - 1] == r2_lines[j]:
                    #this two nodes belongs to one subgraph
                    subGraphJsonMap[nodeSubGraph][str(nodeIndex)][str(r1_lines[j][k])] = r1_lines[j][k+1]
                else:
                    #this two nodes belongs to different subgraphs, Border
                    #   first fills in Fi.I
                    #   then  fills in Fi.O
                    if str(nodeIndex) in partitionJsonMap[nodeSubGraphFI]:
                        partitionJsonMap[nodeSubGraphFI][str(nodeIndex)][str(r1_lines[j][k])] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])
                    else:
                        partitionJsonMap[nodeSubGraphFI][str(nodeIndex)] = {}
                        partitionJsonMap[nodeSubGraphFI][str(nodeIndex)][str(r1_lines[j][k])] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])
                    if str(r1_lines[j][k]) in partitionJsonMap[nodeSubGraphFO]:
                        partitionJsonMap[nodeSubGraphFO][str(r1_lines[j][k])][str(nodeIndex)] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])
                    else:
                        partitionJsonMap[nodeSubGraphFO][str(r1_lines[j][k])] = {}
                        partitionJsonMap[nodeSubGraphFO][str(r1_lines[j][k])][str(nodeIndex)] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])  
    with open(SubGraphJson, "w") as ws, open(PartitionJson, "w") as wp:
        ws.write(json.dumps(subGraphJsonMap, sort_keys=True, indent=4, separators=(',', ': ')))
        wp.write(json.dumps(partitionJsonMap, sort_keys=True, indent=4, separators=(',', ': ')))

def main(argv):
    Graph = ''
    MetisPartion = ''
    SubGraphJson = ''
    PartitionJson = '' 
    try:
      opts, args = getopt.getopt(argv,"hg:m:s:p:",["help","graph=", "metispartion=", "subgraphjson=", "partitionjson="])
    except getopt.GetoptError:
      print 'Usage : generate_subgraph_json.py -g <graph> -m <metispartion> -s <subgraphjson=> -p <partitionjson>, reference -h or --help'
      sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print "this script is for generate subGraph, subGraphFI, subGraphFO json file"
            print "    -g, --graph   the graph file after preprocess"
            print "    -m, --metispartion   the metis partition result"
            print "    -s, --subgraphjson the Json file for partition subGraph"
            print "    -p, --partitionjson the Json file for partition  subGraphFI, subGraphFO"
            sys.exit()
        elif opt in ("-g", "--graph"):
            Graph = arg
        elif opt in ("-m", "--metispartion"):
            MetisPartion = arg
        elif opt in ("-s", "--subgraphjson"):
            SubGraphJson = arg
	elif opt in ("-p", "--partitionjson"):
	    PartitionJson = arg
    generate_json(Graph, MetisPartion, SubGraphJson, PartitionJson)
if __name__ == "__main__":
    main(sys.argv[1:])


