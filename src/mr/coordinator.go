package mr

import (
	"6.824/kvraft"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	nReduceTasks int
	mMapTasks    int
	mapTasks     []Task
	reduceTasks  []Task
}

const workerTimeout = 10
const TempDir = "tmp"

type Task struct {
	WorkerId   string
	TaskType   TaskType
	TaskStatus WorkerStatus
	Filename   string
	TaskId     int
}
type TaskType int
type WorkerStatus int

const (
	Idle WorkerStatus = iota
	In_Progress
	Completed
)
const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	NoTask
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	log.Printf("dekh dekh : mapf : %v %v,,,, reducef : %v %v", c.mMapTasks, len(c.mapTasks), c.nReduceTasks, len(c.reduceTasks))
	task := Task{}
	task.WorkerId = args.WorkerId
	if c.mMapTasks > 0 {
		task = getTask(c.mapTasks)
		reply.Task = task
		c.mapTasks[task.TaskId].TaskStatus = In_Progress
	} else if c.nReduceTasks > 0 {
		task = getTask(c.reduceTasks)
		reply.Task = task
		c.reduceTasks[task.TaskId].TaskStatus = In_Progress
	} else {
		task = Task{
			WorkerId: args.WorkerId,
			TaskType: ExitTask,
			Filename: "",
			TaskId:   0,
		}
		reply.Task = task
	}
	c.mu.Unlock()
	go c.monitorTask(task)
	return nil
}

func getTask(tasks []Task) Task {
	for i := 0; i < len(tasks); i++ {
		if tasks[i].TaskStatus == Idle {
			return tasks[i]
		}
	}
	return Task{TaskType: NoTask}
}

func (c *Coordinator) monitorTask(task Task) {
	time.Sleep(time.Second * workerTimeout)
	c.mu.Lock()
	defer c.mu.Unlock()

	if task.TaskType == MapTask && c.mapTasks[task.TaskId].TaskStatus == In_Progress {
		log.Printf("dhoom machale dhoom : %v", task)
		c.mapTasks[task.TaskId].TaskStatus = Idle
		task.WorkerId = ""
	} else if task.TaskType == ReduceTask && c.reduceTasks[task.TaskId].TaskStatus == In_Progress {
		log.Printf("dhoom machale dhoom : %v", task)
		c.reduceTasks[task.TaskId].TaskStatus = Idle
		task.WorkerId = ""
	}
}

func (c *Coordinator) checkIfNewWorkerAssigned(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MapTask {
		if c.mapTasks[args.TaskId].WorkerId != args.WorkerId {
			reply.CanExit = true
			return nil
		}
	}
	if args.TaskType == ReduceTask {
		if c.reduceTasks[args.TaskId].WorkerId != args.WorkerId {
			reply.CanExit = true
			return nil
		}
	}
	reply.CanExit = false
	return nil
}
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()
	//log.Printf("halla bol %v %v", args.TaskStatus, c.mapTasks[args.TaskId].TaskStatus)
	if args.TaskType == MapTask {
		if c.mapTasks[args.TaskId].TaskStatus == In_Progress {
			c.mMapTasks--
			c.mapTasks[args.TaskId].TaskStatus = Completed
		}
		reply.CanExit = c.mapTasks[args.TaskId].WorkerId == args.WorkerId
		return nil

	} else if args.TaskType == ReduceTask {
		if c.reduceTasks[args.TaskId].TaskStatus == In_Progress {
			c.nReduceTasks--
			c.reduceTasks[args.TaskId].TaskStatus = Completed
		}
		reply.CanExit = c.reduceTasks[args.TaskId].WorkerId == args.WorkerId
		return nil
	} else {
		fmt.Printf("Incorrect task type to report: %v\n", args.TaskType)
		reply.CanExit = false
		return nil
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.ReduceCount = len(c.reduceTasks)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mMapTasks == 0 && c.nReduceTasks == 0
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here
	c.nReduceTasks = nReduce
	c.mMapTasks = len(files)
	kvraft.DPrintf("note it %v-%v", nReduce, c.mMapTasks)
	for i := 0; i < len(files); i++ {
		c.mapTasks = append(c.mapTasks, Task{Filename: files[i], TaskType: MapTask, TaskId: i})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{Filename: strconv.Itoa(i), TaskType: ReduceTask, TaskId: i})
	}
	c.server()
	outFiles, _ := filepath.Glob("mr-out*")
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &c
}
