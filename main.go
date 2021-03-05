package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

const K = 100
const N = 50 // 测试文件大小为10G, 分成50份
const mb = 1024 * 1024
const gb = 1024 * mb

const Debug = 1
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var h 	  *heap
var heaps []*heap
var hashCh [N]chan string

func writeHttp() {
	var err error
	var files [N]*os.File
	finish := make(chan bool, N)
	for i := 0; i < N; i++ {
		fn := fmt.Sprintf("./tmp/%d.txt", i)
		files[i], err = os.Create(fn)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			os.Exit(1)
		}
		go func(x int) {
			for {
				DPrintf("receive data")
				v, ok := <- hashCh[x]
				if !ok { break }
				files[x].WriteString(v + "\n")
			}
			finish <- true
		}(i)
	}
	defer close(finish)
	for i := 0; i < N; i++ {
		<- finish
	}
}

func preprocess() {
	// 创建存放小文件的目录
	os.Mkdir("./tmp", 0755)
	var err error
	var files [N]*os.File
	for i := 0; i < N; i ++ {
		hashCh[i] = make(chan string, 10)
		DPrintf("create channel %d", i)
		fn := fmt.Sprintf("./tmp/%d", i)
		files[i], err = os.Create(fn)
		if err != nil {
			fmt.Printf("Error: %s\n", err)
			os.Exit(1)
		}
		DPrintf("create file %d", i)
	}
	f, _ := os.Open("10gb")
	ReadFile(f)
}

func Map(filename string) {
	kvs := make(map[string]int)
	f, err := os.Open(filename)
	if err != nil {
		fmt.Println(err.Error)
		os.Exit(1)
	}
	defer f.Close()
	h := &heap{}
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
		h.Push(kv{k, v})
		if h.Len() > K {
			h.Pop()
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
			file := "./tmp/" + strconv.Itoa(len(heaps) + i)
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
			kv := heaps[i].Pop().(kv)
			h.Push(kv)
			if h.Len() > 100 {
				h.Pop()
			}
		}
	}
	for i := 0; i < h.Len(); i ++ {
		fmt.Printf("%s %d\n", (*h)[i].k, (*h)[i].v)
	}
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32())
}

func ReadFile(f *os.File) error {
	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 250*1024) // 250mb
		return lines
	}}
	//读取回来的数据池
	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}
	//一个文件对象本身是实现了io.Reader的
	//使用bufio.NewReader去初始化一个Reader对象，存在buffer中的，读取一次就会被清空
	r := bufio.NewReader(f)
	var wg sync.WaitGroup
	for {
		buf := linesPool.Get().([]byte)
		//读取Reader对象中的内容到[]byte类型的buf中
		n, err := r.Read(buf)
		buf = buf[:n]

		if n == 0 {
			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
			return err
		}
		nextUntillNewline, err := r.ReadBytes('\n')
		if err != io.EOF {
			buf = append(buf, nextUntillNewline...)
		}
		wg.Add(1)
		go func() {
			ProcessChunk(buf, &linesPool, &stringPool)
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

func ProcessChunk(chunk []byte, linesPool *sync.Pool, stringPool *sync.Pool) {
	var wg2 sync.WaitGroup
	https := stringPool.Get().(string)
	https = string(chunk)
	linesPool.Put(chunk)
	httpsSlice := strings.Split(https, "\n")
	stringPool.Put(https)
	chunkSize := 500
	n := len(httpsSlice)
	noOfThread := n / chunkSize
	if n%chunkSize != 0 {
		noOfThread++
	}
	for i := 0; i < noOfThread; i++ {
		wg2.Add(1)
		go func(s int, e int) {
			defer wg2.Done() //to avoid deadlocks
			for i := s; i < e; i++ {
				text := httpsSlice[i]
				if len(text) == 0 {
					continue
				}
				id := ihash(text) % N
				DPrintf("send data")
				hashCh[id] <- text
			}
		}(i*chunkSize, min((i+1)*chunkSize, len(httpsSlice)))
	}
	wg2.Wait()
	httpsSlice = nil
}

func main() {
	preprocess()
	MapAll()
	process()
}





