import sys

def transferTime(timeStr):
    time = timeStr.split(":")
    a = int(time[2]) + 60 * int(time[1]) + 3600 * int(time[0])
    return a

filename = sys.argv[1]
f = open(filename)

m = {}

startInc = 0
for line in f:
    if 'start IncEval' in line:
        time = line.split(" ")[1]
        startInc = transferTime(time)

    if "dosen't update in the round" in line:
        line = line.split(' ')
        id = int(line[4])
        endTime = line[1]
        endTime = transferTime(endTime)
        m[id].append(endTime - startInc)

    if "duration time of partial evaluation:" in line:
        line = line.split('\n')[0]
        line = line.split(" ")
        id = int(line[3])
        time = float(line[9])
        if id not in m.keys():
            m[id] = []
        m[id].append(time)

iter = len(m[1])
f.close()
f = open('time.csv', "w+")
f.write("time")
for i in range(1, iter + 1):
    f.write("," + str(i))
f.write('\n')
for id in m.keys():
    f.write(str(id))
    for v in m[id]:
        f.write(',' +  str(v))
    f.write('\n')
f.close()

