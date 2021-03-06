package main

type kv struct {
	k string
	v int
}

type myheap []kv

func (h myheap) Len() int {
	return len(h)
}

func (h myheap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h myheap) Less(i, j int) bool {
	return h[i].v < h[j].v
}


func (h *myheap) Pop() interface{} {
	n := len(*h)
	x := (*h)[n - 1]
	*h = (*h)[:n - 1]
	return x
}

func (h *myheap) Push(x interface{}) {
	*h = append(*h, x.(kv))
}