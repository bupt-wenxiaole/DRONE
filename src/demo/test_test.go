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
	f, err := os.Open("c:\\Users\\zpltys\\Desktop\\WikiTalk.txt")
	defer f.Close()

	if err != nil {
		fmt.Println("read file error")
		return
	}

	bio := bufio.NewReader(f)

	for i := 1; i <= 4; i++ {
		 bio.ReadString('\n')
	}

	n := 2394385
	m := 5021410

	fmt.Println("start read..")

	g := GenerateGraph(n)
	for i := 1; i <= m; i++ {
		line, _ := bio.ReadString('\n')

		s := strings.Split(line, "\t")
		u, _ := strconv.Atoi(s[0])
		v, _ := strconv.Atoi(s[1][0:len(s[1]) - 2])

		g.Insert(u, v, 1)
	}

	fmt.Println("finish insert")

	file, _ := os.OpenFile("c:\\Users\\zpltys\\Desktop\\graph.txt", os.O_RDWR|os.O_CREATE, os.ModeType)
	defer file.Close()

	for i := 0; i < n; i++ {
		e := g.Query(i)
		val := make([]string, 0)
		for _, v := range e {
			val = append(val, strconv.Itoa(v.v))
		}
		file.WriteString(strings.Join(val, ",") + "\n")
	}
}
