#!/bin/bash
# use awk to automatically read config.txt
# set the IP of slaves and the ssh user name
partitionNum="4"
slaveIp1="10.2.152.24"
slaveID1= "1"
slaveIp2="10.2.152.22"
slaveID2= "2"
slaveIp3="10.2.152.21"
slaveID3= "3"
slaveIp4="10.2.152.23"
slaveID4= "4"
slaveName=$(whoami)

# set the golang working directory containing the code
# set the environment variable for golang
source ~/.bash_profile
slavePath=$GOPATH
masterPath=$GOPATH

# change the relative directory so the input files can be open correctly
cd ${masterPath}/src/

# start the master daemon and return immediately

/usr/local/go/bin/go run master.go

# start shell script to start the worker daemon
ssh ${slaveName}@${slaveIp1} "chmod +x ${slavePath}/sbin/startWorker.sh"
ssh ${slaveName}@${slaveIp1} "${slavePath}/sbin/startWorker.sh ${slaveID1} ${partitionNum} &"

ssh ${slaveName}@${slaveIp2} "chmod +x ${slavePath}/sbin/startWorker.sh"
ssh ${slaveName}@${slaveIp2} "${slavePath}/sbin/startWorker.sh ${slaveID2} ${partitionNum} &"

ssh ${slaveName}@${slaveIp3} "chmod +x ${slavePath}/sbin/startWorker.sh"
ssh ${slaveName}@${slaveIp3} "${slavePath}/sbin/startWorker.sh ${slaveID3} ${partitionNum} &"

ssh ${slaveName}@${slaveIp4} "chmod +x ${slavePath}/sbin/startWorker.sh"
ssh ${slaveName}@${slaveIp4} "${slavePath}/sbin/startWorker.sh ${slaveID4} ${partitionNum} &"
