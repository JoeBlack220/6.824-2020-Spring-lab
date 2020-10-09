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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkExecutor struct {
	// Your definitions here.
	id             string
	mapf           func(string, string) []KeyValue
	reducef        func(string, []string) string
	interFileNames []string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := WorkExecutor{}
	worker.reducef = reducef
	worker.mapf = mapf
	worker.id = workerID()

	for true {
		ret := worker.getATask()
		if ret == false {
			break
		}
		time.Sleep(time.Second * 3)

	}
}

func (w *WorkExecutor) getATask() bool {

	args := GetTaskArgs{}
	args.Worker = w.id

	reply := GetTaskReply{}

	ret := call("Master.GetTask", &args, &reply)

	if !ret {
		return false
	}

	if reply.TaskType == 0 {
		w.executeMapTask(reply.TaskIndex, reply.TaskContent, reply.NReduce)
	} else if reply.TaskType == 1 {
		w.executeReduceTask(reply.TaskIndex, reply.TaskContent, reply.NReduce, reply.NMap)
	}

	return true
}

func (w *WorkExecutor) executeMapTask(taskIndex int, fileName string, nReduce int) {
	w.interFileNames = make([]string, nReduce)
	tempFiles := make([]*os.File, nReduce)
	intermediate := make([][]KeyValue, nReduce)

	for i := 0; i < nReduce; i++ {
		w.interFileNames[i] = "mr-" + strconv.Itoa(taskIndex) + "-" + strconv.Itoa(i)
		file, _ := ioutil.TempFile("./", "tmp-map")
		// file, _ := os.Create(w.interFileNames[i])

		tempFiles[i] = file
		defer tempFiles[i].Close()
	}

	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kva := w.mapf(fileName, string(content))

	for _, kv := range kva {
		reduceKey := ihash(kv.Key) % nReduce
		intermediate[reduceKey] = append(intermediate[reduceKey], kv)
	}

	for i := 0; i < nReduce; i++ {
		enc := json.NewEncoder(tempFiles[i])
		// fmt.Printf(tempFiles[i].Name() + "\n")
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				break
			}
		}
	}

	for i, fileN := range tempFiles {
		os.Rename(fileN.Name(), w.interFileNames[i])
	}

	args := FinishTaskArgs{}
	args.Worker = w.id
	args.TaskType = 0
	args.TaskIndex = taskIndex
	args.TaskContent = fileName

	reply := FinishTaskReply{}

	ret := call("Master.FinishTask", &args, &reply)
	if ret == false || reply.HasSuccess == false {
		for _, fileN := range tempFiles {
			os.Remove(fileN.Name())
		}
	}
}

func (w *WorkExecutor) executeReduceTask(taskIndex int, fileName string, nReduce int, nMap int) {
	outFilename := fmt.Sprintf("mr-out-%d", taskIndex)
	intermediate := []KeyValue{}

	for i := 0; i < nMap; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, taskIndex)
		file, err := os.Open(intermediateFileName)

		if err != nil {
			log.Fatalf("cannot open %v", intermediateFileName)
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

	}

	sort.Sort(ByKey(intermediate))
	ofile, _ := os.Create(outFilename)
	defer ofile.Close()

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
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	args := FinishTaskArgs{}
	args.Worker = w.id
	args.TaskType = 1
	args.TaskIndex = taskIndex
	args.TaskContent = fileName

	reply := FinishTaskReply{}
	call("Master.FinishTask", &args, &reply)

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
