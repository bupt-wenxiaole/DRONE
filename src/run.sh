ps -aux | grep go-build | awk '{print $2}' | xargs kill -9
nohup /usr/local/go/bin/go run worker2.go 1 16 > 1.out &
nohup /usr/local/go/bin/go run worker2.go 2 16 > 2.out &
nohup /usr/local/go/bin/go run worker2.go 3 16 > 3.out &
nohup /usr/local/go/bin/go run worker2.go 4 16 > 4.out &
#nohup /usr/local/go/bin/go run worker2.go 5 20 > 5.out &
