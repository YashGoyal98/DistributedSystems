package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var nReduce int

const TaskInterval = 200

var maxWorkers = 2

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	getReduceCount()

	// Your worker implementation here.

	for {
		//workerArgs defined here
		reply, ok := requestTask()
		if ok {
			//
			reply.Task.TaskStatus = In_Progress
			if reply.Task.TaskType == MapTask {
				processMapTask(reply.Task, mapf)
			} else if reply.Task.TaskType == ReduceTask {
				processReduceTask(reply.Task, reducef)
			} else if reply.Task.TaskType == ExitTask {
				fmt.Println("All tasks are done, worker exiting.")
				return
			} else if reply.Task.TaskType == NoTask {
				time.Sleep(TaskInterval * time.Millisecond)
				continue
			}

		} else {
			fmt.Println("Failed to contact coordinator, worker exiting.")
			return
		}
		time.Sleep(TaskInterval * time.Millisecond)
	}
}
func requestTask() (*WorkerReply, bool) {
	args := WorkerArgs{os.Getpid()}
	reply := WorkerReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)

	return &reply, ok
}

// uncomment to send the Example RPC to the coordinator.
//CallExample()

func getReduceCount() {
	args := GetReduceCountArgs{}
	reply := GetReduceCountReply{}
	ok := call("Coordinator.GetReduceCount", &args, &reply)
	if ok {
		nReduce = reply.ReduceCount
	} else {
		log.Fatalf("Unable to get Reduce Count\n")
	}
}
func processExitTask(task Task) {
	//set the worker who is not working as expected as Idle
	//since we are using threads over here that is handled by go
}

func processReduceTask(task Task, reducef func(string, []string) string) {
	log.Printf("hera hera reduce2")
	prefix := fmt.Sprintf("%v/mr", TempDir)
	fileName := fmt.Sprintf("%v-%v", prefix, task.TaskId)
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Error: %v opening Filename Reduce : %v", err, fileName)
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	var kva []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	sort.Sort(ByKey(kva))
	writeReducedOutput(kva, task.TaskId, reducef)
	reportTask(task)
}

func writeReducedOutput(kva []KeyValue, taskId string, reducef func(string, []string) string) {
	oname := fmt.Sprintf("mr-out-%v", taskId)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
}

func processMapTask(task Task, mapf func(string, string) []KeyValue) {
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", task)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return
	}
	file.Close()
	kva := mapf(filename, string(content))
	mapWriteToTemp(kva, task.WorkerId)
	log.Printf("worker : %v completed map task successsfully\n", task.WorkerId)
	reportTask(task)

}

func reportTask(task Task) {
	args := ReportTaskArgs{}
	args.WorkerId = task.WorkerId
	args.TaskId = task.TaskId
	args.TaskType = task.TaskType
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		if reply.CanExit {
			log.Printf("worker : %v reported %v task successsfully\n", task.WorkerId, task.TaskId)

		} else {
			log.Fatal("Failure in reporting Map task\n")
		}

	} else {
		fmt.Printf("call failure %v!\n")
	}
}

func mapWriteToTemp(kva []KeyValue, mapId int) {

	files := make([]*os.File, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)
	prefix := fmt.Sprintf("%v/mr", TempDir)

	for i := 0; i < nReduce; i++ {

		newFilePath := fmt.Sprintf("%v-%v", prefix, i)
		file, err := openFileAppendOrCreate(newFilePath)
		if err != nil {
			fmt.Printf("Cannot create file %v due to : \n ", i)
			fmt.Println(err)
			return
		}
		files = append(files, file)
		encoders = append(encoders, json.NewEncoder(files[i]))

	}

	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		err := encoders[index].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

	//for i, buffer := range buffers {
	//	if err := buffer.Flush(); err != nil {
	//		log.Fatalf("Error : %v during flushing : %v", err, files[i].Name())
	//	}
	//}

	for _, file := range files {
		err := file.Close()
		if err != nil {
			log.Fatalf("error closing file : %v\n", file.Name())
		}
	}
}

func openFileAppendOrCreate(filePath string) (*os.File, error) {
	// Open the file in append mode. If it doesn't exist, create it.
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	log.Printf("hera hera reduce5.3")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	log.Printf("hera hera reduce5.4")
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
