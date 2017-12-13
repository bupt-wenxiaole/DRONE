package main

import (
	"os"
	"log"
	"bufio"
	"strings"
	//"net"
	"io"
	"bytes"
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

	b := bytes.NewBuffer(make([]byte, 0))

	//listen
	//   port, err := strconv.Atoi(strings.Split(w.peers[w.selfId], ":")[1])
	//  if err != nil {
	//        fmt.Println("self ip:port" + w.peers[w.selfId])
	//   }

	for _, l := range lines {
		fmt.Fprintln(b, l)
		fmt.Printf("len(b): %v\n", b.Len())
		fmt.Println(b)

		//fmt.Println(l)


	}

}
