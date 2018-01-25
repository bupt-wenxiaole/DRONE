import sys
a = open('config.txt', 'w')

num = int(sys.argv[1])
m = num / 4

workerId = 1

a.write("0,10.2.152.24:15000\n")

for i in range(1, num + 1):
    a.write("{0},10.2.152.2{1}:{2}\n".format(i, int((i - 1) // m + 1), 15000 + i))


a.close()