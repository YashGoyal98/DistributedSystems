package mr

import (
	"6.824/kvraft"
	"fmt"
	"github.com/golang-collections/collections/set"
	"log"
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
	mu             sync.Mutex
	nReduceTasks   int
	mMapTasks      int
	mapTasks       []Task
	reduceTasks    []Task
	tasksCompleted bool
}

const workerTimeout = 10

var mapTaskSet = set.Set{}
var reduceTaskSet = set.Set{}

type Task struct {
	WorkerId   int
	TaskType   TaskType
	TaskStatus WorkerStatus
	Filename   string
	TaskId     string
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
	defer c.mu.Unlock()
	task := Task{}
	if c.mMapTasks > 0 {
		kvraft.DPrintf("Map task started")
		if len(c.mapTasks) != 0 {
			task = Task{
				WorkerId: args.WorkerId,
				TaskType: MapTask,
				Filename: c.mapTasks[0].Filename,
				TaskId:   c.mapTasks[0].TaskId,
			}
			if len(c.mapTasks) == 1 {
				c.mapTasks = nil
			} else {
				c.mapTasks = c.mapTasks[1:]
			}
			reply.Task = task
			go c.monitorTask(task)
			return nil
		}
	} else if c.nReduceTasks > 0 {
		kvraft.DPrintf("Reduce task started")
		if len(c.reduceTasks) != 0 {
			task = Task{
				WorkerId: args.WorkerId,
				TaskType: ReduceTask,
				Filename: c.reduceTasks[0].Filename,
				TaskId:   c.reduceTasks[0].TaskId,
			}
			if len(c.reduceTasks) == 1 {
				c.reduceTasks = nil
			} else {
				c.reduceTasks = c.reduceTasks[1:]
			}
			reply.Task = task
			go c.monitorTask(task)
			return nil
		}
	} else if c.mMapTasks == 0 && c.nReduceTasks == 0 {
		c.tasksCompleted = true
		return nil
	}

	return nil
}

func (c *Coordinator) monitorTask(task Task) {
	time.Sleep(time.Second * workerTimeout)
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.TaskType == MapTask && mapTaskSet.Has(task.TaskId) {
		task.TaskStatus = Idle
		c.mapTasks = append(c.mapTasks, task)
		c.mMapTasks++
	} else if task.TaskType == ReduceTask && reduceTaskSet.Has(task.TaskId) {
		task.TaskStatus = Idle
		c.reduceTasks = append(c.reduceTasks, task)
		c.nReduceTasks++

	}
}
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == MapTask {
		c.mMapTasks--
		mapTaskSet.Insert(args.TaskId)
		log.Printf("dhak dhak %v-%v\n", c.mMapTasks, len(c.mapTasks))
		reply.CanExit = true
		return nil
	} else if args.TaskType == ReduceTask {
		c.nReduceTasks--
		reduceTaskSet.Insert(args.TaskId)
		reply.CanExit = true
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
	kvraft.DPrintf("here")
	err := os.Remove(sockname)
	if err != nil {
		return
	}
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
	return c.tasksCompleted
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
	c.tasksCompleted = false
	for i := 0; i < len(files); i++ {
		c.mapTasks = append(c.mapTasks, Task{Filename: files[i], TaskType: MapTask, TaskId: strconv.Itoa(i)})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{Filename: strconv.Itoa(i), TaskType: ReduceTask, TaskId: strconv.Itoa(i)})
	}
	c.server()
	return &c
}
