package main

import (
	"bufio"
	"container/heap"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"time"
	"sync"
)

const K = 100
const N = 100
const MB = 1024 * 1024 
const GB = 1024 * MB

// 存放 url
var urls chan string
// 存放对应哈希
var hashs [N]chan string
// url 文件名
var url_file string
// 存放每个小文件的小根堆
var heaps []*myheap
// 大文件的小根堆
var h_sum *myheap

// 字符串哈希
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % N
}

// 根据哈希值获取文件名
func file_name(x int) string {
	return fmt.Sprintf("./tmp/%d", x)
}

// 初始化
func init() {
	// 创建临时目录
	os.Mkdir("./tmp", 0755)
	// 全局变量初始化
	urls = make(chan string, 1e5)
	for i := 0; i < N; i++ {
		hashs[i] = make(chan string, 1000)
	}
}

// 从指定文件中逐行读取 url，并写入 chan
func read(file string, c chan string) {
	defer close(c)
	f, err := os.Open(file)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	defer f.Close()
	var current int64
	var limit int64 = 100 * MB
	var nThread int = int(1 * GB / limit + 1)
	rw := sync.WaitGroup{}
	for i := 0; i < nThread; i ++ {
		rw.Add(1)
		go func(x int) {
			defer rw.Done()
			readThread(current, limit, file, urls)
			fmt.Printf("read %d thread has been completed \n", x)
		}(i)
	}
	rw.Wait()	
}

func readThread(offset int64, limit int64, fileName string, channel chan string) {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	// 将文件指针移动到指定块的开始位置。
	file.Seek(offset, 0)
	reader := bufio.NewReader(file)
	var cummulativeSize int64
	for {
		// 如果读大小超过了块大小，则断开。
		if cummulativeSize > limit {
			break
		}
		b, _, err := reader.ReadLine()
		// 如果遇到文件结束，则中断。
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		cummulativeSize += int64(len(b))
		s := string(b)
		if s != "" {
			// 将通道中的已读单词发送到字典中。
			channel <- s
		}
	}
}


// 根据 url 哈希分组，并写入对应 chan
func make_group() {
	for {
		v, ok := <- urls
		if !ok { break }
		hashs[ihash(v)] <- v
	}
	for i := 0; i < N; i++ {
		close(hashs[i])
	}
} 

// 将分好组的 url 写入对应的小文件中
func wait_write() {
	var err error
	var files [N]*os.File
	w := sync.WaitGroup{}
	for i := 0; i < N; i++ {
		fn := file_name(i)
		files[i], err = os.Create(fn)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			os.Exit(1)
		}
		w.Add(1)
		go func(x int) {
			defer w.Done()
			for {
				v, ok := <- hashs[x]
				if !ok { break }
				files[x].Write(append([]byte(v), '\n'))
			}
		}(i)
	}
	w.Wait()
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
		for i := 0; i < 10; i ++ {
			wg.Add(1)
			file := fmt.Sprintf("./tmp/%d", len(heaps) + i)
			go func() {
				defer wg.Done()
				Map(file)
			}()
		}
		wg.Wait()
	}
}

func process() {
	h_sum = &myheap{}
	for i := 0; i < len(heaps); i ++ {
		for heaps[i].Len() > 0 {
			kv := heap.Pop(heaps[i]).(kv)
			heap.Push(h_sum, kv)
			if h_sum.Len() > 100 {
				heap.Pop(h_sum)
			}
		}
	}
	for i := 0; i < h_sum.Len(); i ++ {
		fmt.Printf("%s %d\n", (*h_sum)[i].k, (*h_sum)[i].v)
	}
}

func elapsed(what string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", what, time.Since(start))
	}
}

func main() {
	defer elapsed("program")()

	file := flag.String("file", "", "big url file path")
	flag.Parse()

	go read(*file, urls)
	go make_group()
	wait_write()
	MapAll()
	process()
}