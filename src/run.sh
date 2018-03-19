ps -aux | grep go-build | awk '{print $2}' | xargs kill -9
nohup /usr/local/go/bin/go run worker2.go 9 16 &
nohup /usr/local/go/bin/go run worker2.go 10 16 &
nohup /usr/local/go/bin/go run worker2.go 11 16 &
nohup /usr/local/go/bin/go run worker2.go 12 16 &
#nohup /usr/local/go/bin/go run worker2.go 15 20 &
