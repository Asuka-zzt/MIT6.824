package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerId := os.Getpid()
	alive := true
	attempt := 0

	for alive {
		attempt++
		fmt.Println(workerId, " >worker ask", attempt)

		job := RequireTask(workerId)
		fmt.Println(workerId, "worker get job", job)
		switch job.JobType {
		case MapJob:
			{
				DoMap(mapf, job)
				fmt.Println("do map", job.JobId)
				JobIsDone(workerId, job)
			}
		case ReduceJob:
			if job.JobId >= 8 {
				DoReduce(reducef, job)
				fmt.Println("do reduce", job.JobId)
				JobIsDone(2, job)
			}

		case WaittingJob:
			fmt.Println("get waiting")
			time.Sleep(time.Second)
		case KillJob:
			{
				time.Sleep(time.Second)
				fmt.Println("[Status] :", workerId, "terminated........")
				alive = false
			}
		}
		time.Sleep(time.Second)

	}

	//CallForMap(workerId)

}

func RequireTask(workerId int) *Job {
	args := ExampleArgs{}

	reply := Job{}

	call("Coordinator.DistributeJob", &args, &reply)
	fmt.Println("get response", &reply)

	return &reply

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	// 服务类的方法名 请求参数 返回值
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

func JobIsDone(workerId int, job *Job) {
	args := job
	reply := &ExampleReply{}
	call("Coordinator.JobIsDone", &args, &reply)
}

func DoMap(mapf func(string, string) []KeyValue, response *Job) {
	var intermediate []KeyValue
	filename := response.InputFile[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))

	//initialize and loop over []KeyValue
	rn := response.ReducerNum
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.JobId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}

}

func DoReduce(reducef func(string, []string) string, response *Job) {
	reduceFileNum := response.JobId
	intermediate := readFromLocalFile(response.InputFile)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), oname)
}

func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}
