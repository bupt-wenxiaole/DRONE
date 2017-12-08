package demo

import (
	"fmt"
	"testing"
	"os"
	"bufio"
	"strings"
	"strconv"
)

func TestStoreGraph(t *testing.T) {
	f, err := os.Open("/home/zpltys/code/USA-road-d.USA.gr")
	defer f.Close()

	if err != nil {
		fmt.Println("read file error")
		return
	}

	bio := bufio.NewReader(f)

	for i := 1; i <= 7; i++ {
		 bio.ReadString('\n')
	}

	n := 23947348
	m := 58333344
    //m := 10
	fmt.Println("start read..")

	g := GenerateGraph(n)
	for i := 1; i <= m; i++ {
		line, _ := bio.ReadString('\n')

		s := strings.Split(line, " ")
		u, _ := strconv.Atoi(s[1])
		v, _ := strconv.Atoi(s[2])
		w, _ := strconv.Atoi(s[3][0:len(s[3]) - 1])

		//fmt.Println(i, u, v, w)
		if i % 1000000 == 0 {
			fmt.Printf("finished %v edge", i)
		}
		g.Insert(u, v, w)
	}

	fmt.Println("finish insert")

	file, _ := os.OpenFile("/home/zpltys/code/graph.txt", os.O_RDWR|os.O_CREATE, os.ModeType)
	defer file.Close()

	for i := 0; i < n; i++ {
		e := g.Query(i)
		val := make([]string, 0)
		for _, v := range e {
			val = append(val, strconv.Itoa(v.v))
		}
		file.WriteString(strings.Join(val, ",") + "\n")
		if i % 1000000 == 0 {
			fmt.Printf("query %v nodes", i)
		}
	}
}
