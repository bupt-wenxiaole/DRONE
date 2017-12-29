for ((i=$1; i<=$2;++i))
do
    alluxio fs copyFromLocal G$3_$i.json /GRAPE/simData/
    alluxio fs copyFromLocal P$3_$i.json /GRAPE/simData/
done
