import json
import sys
import getopt

def generate_json(Graph, MetisPartion, JsonFile):
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
            for k in range(0, len(r1_lines[j]), 2):
                # K means another node in pair 
                if r2_lines[r1_lines[j][k] - 1] == r2_lines[j]:
                    #this two nodes belongs to one subgraph
                    subGraphJsonMap[nodeSubGraph][str(nodeIndex)][str(r1_lines[j][k])] = r1_lines[j][k+1]
                else:
                    #this two nodes belongs to different subgraphs, Border
                    #   first fills in Fi.I
                    #   then  fills in Fi.O
                    if str(nodeIndex) in subGraphJsonMap[nodeSubGraphFI]:
                        subGraphJsonMap[nodeSubGraphFI][str(nodeIndex)][str(r1_lines[j][k])] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])
                    else:
                        subGraphJsonMap[nodeSubGraphFI][str(nodeIndex)] = {}
                        subGraphJsonMap[nodeSubGraphFI][str(nodeIndex)][str(r1_lines[j][k])] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])
                    if str(r1_lines[j][k]) in subGraphJsonMap[nodeSubGraphFO]:
                        subGraphJsonMap[nodeSubGraphFO][str(r1_lines[j][k])][str(nodeIndex)] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])
                    else:
                        subGraphJsonMap[nodeSubGraphFO][str(r1_lines[j][k])] = {}
                        subGraphJsonMap[nodeSubGraphFO][str(r1_lines[j][k])][str(nodeIndex)] = str(r1_lines[j][k+1]) + " " + str(r2_lines[r1_lines[j][k] - 1])  
    with open(JsonFile, "w") as w:
        w.write(json.dumps(subGraphJsonMap, sort_keys=True, indent=4, separators=(',', ': ')))
        

def main(argv):
    Graph = ''
    MetisPartion = ''
    JsonFile = ''
    try:
      opts, args = getopt.getopt(argv,"hg:m:o:",["help","graph=","metispartion=","outputfile="])
    except getopt.GetoptError:
      print 'Usage : generate_subgraph_json.py -g <graph> -m <metispartion> -o <outputfile>, reference -h or --help'
      sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print "this script is for generate subGraph, subGraphFI, subGraphFO json file"
            print "    -g, --graph   the graph file after preprocess"
            print "    -m, --metispartion   the metis partition result"
            print "    -o, --outputfile the Json file for partition subGraph, subGraphFI, subGraphFO"
            sys.exit()
        elif opt in ("-g", "--graph"):
            Graph = arg
        elif opt in ("-m", "--metispartion"):
            MetisPartion = arg
        elif opt in ("-o", "--outputfile"):
            JsonFile = arg
    generate_json(Graph, MetisPartion, JsonFile)
if __name__ == "__main__":
    main(sys.argv[1:])


