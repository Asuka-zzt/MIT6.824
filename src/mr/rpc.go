package mr

import (
	"os"
	"strconv"
)

// 任务类型
type JobType int

const (
	MapJob JobType = iota
	ReduceJob
	WaitingJob // 此时没有任务分配，Worker 请稍作等待
	KillJob    // 全部完成，Worker 退出
)

// 任务状态
type JobCondition int

const (
	JobWaiting JobCondition = iota // 任务未分配
	JobWorking                     // 任务正在进行
	JobDone                        // 任务已完成
)

// Coordinator 的状态
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

// RPC 参数：Worker 请求任务
type GetJobArgs struct{}

// RPC 响应：分配给 Worker 的任务
type Job struct {
	JobType    JobType
	JobId      int
	InputFile  []string // Map 任务是输入文件，Reduce 任务不需要这里传文件列表，Worker 自己去 Glob
	ReducerNum int      // Map 任务需要知道有多少个 Reducer
	MapNum     int      // Reduce 任务需要知道有多少个 Map (用于构建文件名)
}

// RPC 参数：Worker 报告任务完成
type JobDoneArgs struct {
	JobType JobType
	JobId   int
}

type JobDoneReply struct{}

// 生成 Socket 名称 (用于 UNIX Socket 通信，测试脚本要求)
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
