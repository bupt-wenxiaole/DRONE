package main

import (
	"os"
	"log"
	"bufio"
	"strings"
	"net"
	"io"
	"fmt"
)

func main() {
	lines := make([]string, 0)

	f, err := os.Open("../test_data/config.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadString('\n')
		line = strings.Split(line, "\n")[0]
		if err != nil || io.EOF == err {
			break
		}

		conf := strings.Split(line, ",")
		lines = append(lines, conf[1])
	}

	//listen
	//   port, err := strconv.Atoi(strings.Split(w.peers[w.selfId], ":")[1])
	//  if err != nil {
	//        fmt.Println("self ip:port" + w.peers[w.selfId])
	//   }

	for _, l := range lines {
        port := strings.Split(l, ":")[1]
        fmt.Println(port)

		ln, err := net.Listen("tcp", ":" + port)
		if err != nil {
			panic(err)
		}
		ln.Close()
	}

}
