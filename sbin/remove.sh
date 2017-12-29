for ((i=$1; i<=$2;++i))
do
    alluxio fs rm /GRAPE/simData/G$3_$i.json
    alluxio fs rm /GRAPE/simData/P$3_$i.json
done
