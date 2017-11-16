with open("small_metis_graph", "r") as r1 , open("small_metis_graph.part.2", "r") as r2:
    #r1.next()
    r1_lines = r1.readlines()
    r1_lines = r1_lines[1:]
    for i in range(len(r1_lines)):
        r1_lines[i] = r1_lines[i].strip().split()
    r2_lines = r2.readlines()
    r2_lines = map(int, r2_lines)
    assert len(r1_lines) == len(r2_lines) 
    print r1_lines
    print r2_lines
    partition_num = 2
    subGraphJsonMap = {}
    for i in range(partition_num):
        subGraph = "Graph" + str(i)
        subGraphJsonMap[subGraph] = {}
        for j in range(len(r1_lines)):
            nodeIndex = i + 1
            subGraphJsonMap[subGraph][str(nodeIndex)] = {}
            for j in range(0, len(r1_lines[i]), 2):
                if r2_lines[r1_lines[i][j]] == i:
                    subGraphJsonMap[subGraph][str(nodeIndex)][str(r1_lines[j])] = str(r1_lines[j+1])
    print subGraphJsonMap



