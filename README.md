# topKURL
PingCAP Interview
## 要求描述
100GB url 文件，使用 1GB 内存计算出出现次数 top100 的 url 和出现的次数。
## 实现思路
用多线程并发读大文件用管道传送url并发写入对应的小文件中（用哈希函数分配）。
多线程读取小文件做数据统计写入对应的大小为100的小根堆中。最后遍历所有小根堆得到最终结果。
## 使用方法
python generate.py 1 1gb
go run . --file = 1gb
