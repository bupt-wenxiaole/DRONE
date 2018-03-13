for ((i=$1; i<=$2;++i))
do
    rm $i.out
    nohup /usr/local/go/bin/go run ../src/worker3.go $i $3 > $i.out &
done
