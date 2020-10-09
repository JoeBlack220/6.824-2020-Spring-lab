package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type TaskStatus int32

const (
	UNASSIGNED TaskStatus = 0
	ASSIGNED   TaskStatus = 1
	FINISHED   TaskStatus = 2
)

type Master struct {
	// Your definitions here.
	mapTaskNames     []string
	mapTaskStatus    []TaskStatus
	mapTaskWorker    []string
	reduceTaskStatus []TaskStatus
	reduceTaskWorker []string
	mapTaskLeft      int
	reduceTaskLeft   int
	mu               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
// If there is an unassigned map task, assign a map task
// Otherwise, if all the map task is finished, assign a reduce task
// If all map tasks are assigned but not finished, return an empty task
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	m.mu.Lock()
	defer m.mu.Unlock()
	reply.NReduce = len(m.reduceTaskStatus)
	reply.NMap = len(m.mapTaskStatus)
	reply.TaskType = 2

	// All the tasks are finished
	if m.mapTaskLeft == 0 && m.reduceTaskLeft == 0 {
		return nil
	}

	for i := 0; i < len(m.mapTaskStatus); i++ {
		if m.mapTaskStatus[i] == UNASSIGNED {
			reply.TaskContent = m.mapTaskNames[i]
			reply.TaskIndex = i
			reply.TaskType = 0
			m.mapTaskWorker[i] = args.Worker
			m.mapTaskStatus[i] = ASSIGNED
			go m.endSlowWorker(0, i, args.Worker)
			return nil
		}
	}

	// All the map tasks are finished
	if m.mapTaskLeft == 0 {
		for i := 0; i < len(m.reduceTaskStatus); i++ {
			if m.reduceTaskStatus[i] == UNASSIGNED {
				reply.TaskContent = strconv.Itoa(i)
				reply.TaskIndex = i
				reply.TaskType = 1
				m.reduceTaskWorker[i] = args.Worker
				m.reduceTaskStatus[i] = ASSIGNED
				go m.endSlowWorker(1, i, args.Worker)
				return nil
			}
		}
	}

	return nil
}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.TaskType == 0 {
		if m.mapTaskStatus[args.TaskIndex] != ASSIGNED || args.Worker != m.mapTaskWorker[args.TaskIndex] {
			reply.HasSuccess = false
		} else {
			m.mapTaskStatus[args.TaskIndex] = FINISHED
			m.mapTaskLeft--
			fmt.Printf("Map task " + m.mapTaskNames[args.TaskIndex] + " finished.\n")
			reply.HasSuccess = true
		}
	} else {
		if m.reduceTaskStatus[args.TaskIndex] != ASSIGNED || args.Worker != m.reduceTaskWorker[args.TaskIndex] {
			reply.HasSuccess = false
		} else {
			m.reduceTaskStatus[args.TaskIndex] = FINISHED
			m.reduceTaskLeft--
			fmt.Printf("Reduce task " + strconv.Itoa(args.TaskIndex) + " finished.\n")

			reply.HasSuccess = true
		}
	}

	return nil
}

// taskType 0: map task
// taskType 1: reduce task
func (m *Master) endSlowWorker(taskType int, taskIndex int, worker string) {
	time.Sleep(time.Second * 10)

	m.mu.Lock()
	defer m.mu.Unlock()

	if taskType == 0 {
		if m.mapTaskStatus[taskIndex] == FINISHED {
			return
		} else {
			m.mapTaskStatus[taskIndex] = UNASSIGNED
			m.mapTaskWorker[taskIndex] = ""
		}
	} else {
		if m.reduceTaskStatus[taskIndex] == FINISHED {
			return
		} else {
			m.reduceTaskStatus[taskIndex] = UNASSIGNED
			m.reduceTaskWorker[taskIndex] = ""
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	m.mu.Lock()
	defer m.mu.Unlock()
	// Your code here.
	ret = m.mapTaskLeft == 0 && m.reduceTaskLeft == 0
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.mapTaskNames = files
	m.mapTaskStatus = make([]TaskStatus, len(files))
	m.mapTaskLeft = len(files)
	m.mapTaskWorker = make([]string, len(files))

	m.reduceTaskStatus = make([]TaskStatus, nReduce)
	m.reduceTaskLeft = nReduce
	m.reduceTaskWorker = make([]string, nReduce)
	m.mu = sync.Mutex{}
	// Your code here.

	m.server()
	return &m
}
