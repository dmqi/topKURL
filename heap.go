package main

type kv struct {
	k string
	v int
}

type heap []kv

func (h heap) Len() int {
	return len(h)
}

func (h heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h heap) Less(i, j int) bool {
	return h[i].v < h[j].v
}


func (h *heap) Pop() interface{} {
	n := len(*h)
	x := (*h)[n - 1]
	*h = (*h)[:n - 1]
	return x
}

func (h *heap) Push(x interface{}) {
	*h = append(*h, x.(kv))
}

