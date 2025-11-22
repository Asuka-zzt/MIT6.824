package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// 排序支持
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		// 1. 请求任务
		args := GetJobArgs{}
		reply := Job{}
		ok := call("Coordinator.DistributeJob", &args, &reply)

		if !ok {
			fmt.Println("Coordinator unreachable, exiting.")
			return
		}

		// 2. 根据任务类型处理
		switch reply.JobType {
		case MapJob:
			DoMap(mapf, &reply)
			callDone(MapJob, reply.JobId)

		case ReduceJob:
			DoReduce(reducef, &reply)
			callDone(ReduceJob, reply.JobId)

		case WaitingJob:
			// 没有任务，休息一下再请求
			time.Sleep(time.Second)

		case KillJob:
			return
		}
	}
}

func callDone(typ JobType, id int) {
	args := JobDoneArgs{JobType: typ, JobId: id}
	reply := JobDoneReply{}
	call("Coordinator.JobIsDone", &args, &reply)
}

func DoMap(mapf func(string, string) []KeyValue, job *Job) {
	// 读取输入文件
	filename := job.InputFile[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// 执行 Map 函数
	kva := mapf(filename, string(content))

	// 准备中间文件 Buffer (二维切片)
	buckets := make([][]KeyValue, job.ReducerNum)
	for _, kv := range kva {
		bucketId := ihash(kv.Key) % job.ReducerNum
		buckets[bucketId] = append(buckets[bucketId], kv)
	}

	// 写入磁盘
	for i := 0; i < job.ReducerNum; i++ {
		// 临时文件格式: mr-MapID-ReduceID
		oname := fmt.Sprintf("mr-%d-%d", job.JobId, i)
		// 先写临时文件
		tempFile, _ := ioutil.TempFile("", "mr-map-tmp-*")
		enc := json.NewEncoder(tempFile)
		for _, kv := range buckets[i] {
			enc.Encode(kv)
		}
		tempFile.Close()
		// 原子重命名
		os.Rename(tempFile.Name(), oname)
	}
}

func DoReduce(reducef func(string, []string) string, job *Job) {
	// 读取所有相关的中间文件: mr-*-ReduceID
	// 使用 Glob 模式匹配
	pattern := fmt.Sprintf("mr-*-%d", job.JobId)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("Reduce Glob error: %v", err)
	}

	var intermediate []KeyValue
	for _, filename := range matches {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// 创建最终输出文件 mr-out-ReduceID
	oname := fmt.Sprintf("mr-out-%d", job.JobId)
	tempFile, _ := ioutil.TempFile("", "mr-out-tmp-*")

	// 执行 Reduce
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 格式化输出
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	os.Rename(tempFile.Name(), oname)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
