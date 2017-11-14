import os
def ProcessTextFileToMetisInput(filepath, outfilepath):
    outMap = {}
    numEdges = 0
    with open(filepath, "r") as r:
        for line in r:
            if line.startswith('a'):
                numEdges += 1
                _, startNode, endNode, weight =  line.split(' ')
                startNode = int(startNode)
                endNode = int(endNode)
                weight = int(weight)
                print startNode, endNode, weight
                if startNode in outMap:
                    outMap[startNode].extend([endNode, weight])
                else:
                    outMap[startNode] = []
                    outMap[startNode].extend([endNode, weight])
    with open(outfilepath, "w") as w:
        numNodes = len(outMap)
        w.write(str(numNodes)+ " " + str(numEdges/2) + " " + '001' + os.linesep)
        for i in range(1, numNodes + 1):
            w.write(" ".join(str(elm) for elm in outMap[i]) + os.linesep)
def main():
    inpath = "USA-road-d.USA.gr"
    outpath = "USA-road-metis-Graph"
    ProcessTextFileToMetisInput(inpath, outpath)

if __name__ == "__main__":
    main()