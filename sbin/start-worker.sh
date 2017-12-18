#!/bin/bash

# set the environment variable
source ~/.bash_profile
slavePath=$GOPATH
slaveID=$1
partitionNum=$2
cd ${slavePath}/src/

/usr/local/go/bin/go run worker1.go ${slaveID} ${partitionNum}