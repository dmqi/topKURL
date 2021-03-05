package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sync"
)

const K = 100
const N = 50 // 测试文件大小为10G, 分成50份

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var h 	  *myheap
var heaps []*myheap
var hashCh [N]chan string
var files [N]*os.File

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % N
}

func prepare() {
	os.Mkdir("./tmp", 0755)
	for i := 0; i < N; i ++ {
		hashCh[i] = make(chan string, 100)
	}
}

func ReadFile(filePath string, handle func(string)) {
	f, err := os.Open(filePath)
	defer f.Close()
	if err != nil {
		fmt.Println(err.Error)
		return
	}
	buf := bufio.NewReader(f)

	for {
		line, _, err := buf.ReadLine()
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Printf("Error: %s\n", err)
			return
		}
		handle(string(line))
	}
}

func send(s string) {
	i := ihash(s)
	hashCh[i] <- s
}

func WriteFile() {
	var err error
	wait := sync.WaitGroup{}
	for i := 0; i < N; i++ {
		fn := fmt.Sprintf("./tmp/%d", i)
		files[i], err = os.Create(fn)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			os.Exit(1)
		}
		wait.Add(1)
		go func(x int) {
			defer wait.Done()
			for {
				v, ok := <- hashCh[x]
				// DPrintf("receive data")
				if !ok { break }
				files[x].Write(append([]byte(v), '\n'))
			}
		}(i)
	}
	wait.Wait()
}

func Map(filename string) {
	kvs := make(map[string]int)
	f, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	defer f.Close()
	h := &myheap{}
	heap.Init(h)
	r := bufio.NewReader(f)
	for {
		line, _, err := r.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("Error: %s\n", err)
			os.Exit(1)
		}
		kvs[string(line)] ++;
	}
	for k, v := range kvs {
		heap.Push(h, kv{k, v})
		if h.Len() > K {
			heap.Pop(h)
		}
	}
	heaps = append(heaps, h)
}

func MapAll() {
	for {
		if len(heaps) == N {return}
		wg := sync.WaitGroup{}
		for i := 0; i < 5; i ++ {
			wg.Add(1)
			file := fmt.Sprintf("./tmd/%d", len(heaps) + i)
			go func() {
				Map(file)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func process() {
	for i := 0; i < len(heaps); i ++ {
		for heaps[i].Len() > 0 {
			kv := heap.Pop(heaps[i]).(kv)
			heap.Push(h, kv)
			if h.Len() > 100 {
				heap.Pop(h)
			}
		}
	}
	for i := 0; i < h.Len(); i ++ {
		fmt.Printf("%s %d\n", (*h)[i].k, (*h)[i].v)
	}
}


func main() {
	prepare()
	go ReadFile("10gb", send)
	WriteFile()
	MapAll()
	process()
}




