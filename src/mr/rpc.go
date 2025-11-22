package mr

import (
	"os"
	"strconv"
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type WorkerRequest struct {
	WorkerId string
	WorkDone int
}

// condition of job
type JobCondition int

const (
	JobWorking = iota
	JobWaiting
	JobDone
)

// type of job
type JobType int

const (
	MapJob = iota
	ReduceJob
	WaittingJob
	KillJob
)

// condition of coordinator
type Condition int

const (
	MapPhase = iota
	ReducePhase
	AllDone
)

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
