package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type JobMetaInfo struct {
	condition JobCondition
	StartTime time.Time
	JobPtr    *Job
}

type Coordinator struct {
	mu         sync.Mutex // 锁放入结构体
	files      []string
	ReducerNum int
	MapNum     int
	Phase      Phase
	JobChannel chan *Job            // 统一的任务分发通道
	jobMeta    map[int]*JobMetaInfo // 记录任务状态: Map任务ID 0~MapNum-1, Reduce任务ID 0~ReduceNum-1

	nMapDone    int
	nReduceDone int
}

// RPC Handler: Worker 请求任务
func (c *Coordinator) DistributeJob(args *GetJobArgs, reply *Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Phase == AllDone {
		reply.JobType = KillJob
		return nil
	}

	// 尝试从 Channel 获取任务
	select {
	case job := <-c.JobChannel:
		// 获取到任务，更新元数据
		if meta, ok := c.jobMeta[job.JobId]; ok {
			// 只有当任务不在 JobDone 状态时才分发 (防止重复)
			if meta.condition != JobDone {
				meta.condition = JobWorking
				meta.StartTime = time.Now()

				// 填充回复
				*reply = *job
				// 如果是 Map 任务，单独设置 InputFile
				if job.JobType == MapJob {
					reply.InputFile = []string{c.files[job.JobId]}
				}
			} else {
				// 任务已完成，无需分发，让 Worker 等待
				reply.JobType = WaitingJob
			}
		}
	default:
		// Channel 空了，让 Worker 等待
		reply.JobType = WaitingJob
	}

	return nil
}

// RPC Handler: Worker 报告完成
func (c *Coordinator) JobIsDone(args *JobDoneArgs, reply *JobDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查任务是否存在且正在运行
	if meta, ok := c.jobMeta[args.JobId]; ok {
		// 只有在 Working 状态下报告完成才有效 (防止超时的旧 Worker 回来报告)
		if meta.condition == JobWorking && meta.JobPtr.JobType == args.JobType {
			meta.condition = JobDone

			switch args.JobType {
			case MapJob:
				c.nMapDone++
				fmt.Printf("Map Task %d done. Progress: %d/%d\n", args.JobId, c.nMapDone, c.MapNum)
				if c.nMapDone == c.MapNum {
					c.startReducePhase()
				}
			case ReduceJob:
				c.nReduceDone++
				fmt.Printf("Reduce Task %d done. Progress: %d/%d\n", args.JobId, c.nReduceDone, c.ReducerNum)
				if c.nReduceDone == c.ReducerNum {
					c.Phase = AllDone
					fmt.Println("All tasks done!")
				}
			}
		}
	}
	return nil
}

// 切换到 Reduce 阶段 (由 JobIsDone 内部调用，已持有锁)
func (c *Coordinator) startReducePhase() {
	fmt.Println("Map Phase Finished. Starting Reduce Phase...")
	c.Phase = ReducePhase

	// 生成 Reduce 任务
	// 注意：JobId 对于 Reduce 任务，我们重用 0 到 ReducerNum-1
	// 为了区分 Map 和 Reduce 在 jobMeta 中的 Key，我们可以加上 MapNum 偏移量，
	// 或者简单地清空 jobMeta 重用。这里为了简单，我选择重置 jobMeta。

	// 清空旧的 Map 任务 Channel (如果有残留)
	// 并重新初始化 Channel
	c.JobChannel = make(chan *Job, c.ReducerNum)
	c.jobMeta = make(map[int]*JobMetaInfo)

	for i := 0; i < c.ReducerNum; i++ {
		job := &Job{
			JobType:    ReduceJob,
			JobId:      i,
			ReducerNum: c.ReducerNum,
			MapNum:     c.MapNum,
		}
		c.jobMeta[i] = &JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    job,
		}
		c.JobChannel <- job
	}
}

// 崩溃检测协程
func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(1 * time.Second)

		c.mu.Lock()
		if c.Phase == AllDone {
			c.mu.Unlock()
			return
		}

		var jobsToRetry []*Job

		now := time.Now()
		for _, meta := range c.jobMeta {
			// 如果任务正在工作且超时 (> 10秒)
			if meta.condition == JobWorking && now.Sub(meta.StartTime) > 10*time.Second {
				fmt.Printf("Job %d timed out, resetting...\n", meta.JobPtr.JobId)
				meta.condition = JobWaiting
				jobsToRetry = append(jobsToRetry, meta.JobPtr)
			}
		}
		c.mu.Unlock()

		// 在锁外将任务重新放入 Channel，避免死锁
		for _, job := range jobsToRetry {
			c.JobChannel <- job
		}
	}
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	// 必须使用 unix socket 才能通过 test-mr.sh
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Phase == AllDone
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		ReducerNum: nReduce,
		MapNum:     len(files),
		Phase:      MapPhase,
		JobChannel: make(chan *Job, len(files)), // 缓冲大小足够容纳所有 Map 任务
		jobMeta:    make(map[int]*JobMetaInfo),
	}

	// 初始化 Map 任务
	for i, _ := range files {
		job := &Job{
			JobType:    MapJob,
			JobId:      i,
			ReducerNum: nReduce,
			MapNum:     len(files),
		}
		c.jobMeta[i] = &JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    job,
		}
		c.JobChannel <- job
	}

	c.server()
	go c.CrashHandler()
	return &c
}
