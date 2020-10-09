package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
	Worker string
}

// taskType 0: map task 1: reduce task 2: no task currently
// taskContent: map task: file name, reduce task: task key
type GetTaskReply struct {
	TaskType    int
	TaskIndex   int
	TaskContent string
	NReduce     int
	NMap        int
}

type FinishTaskArgs struct {
	Worker      string
	TaskType    int
	TaskIndex   int
	TaskContent string
}

type FinishTaskReply struct {
	HasSuccess bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// Generate a worker id
func workerID() string {
	s := "mr-worker-"
	s += strconv.Itoa(os.Getpid())
	return s
}
