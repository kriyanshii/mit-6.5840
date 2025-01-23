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

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	map_remain    int
	reduce_remain int
	mtasks        []Task
	rtasks        []Task
}

// Your code here -- RPC handlers for the worker to call.
const (
	IDLE        = 0
	IN_PROGRESS = 1
	COMPLETE    = 2
	MAP         = 0
	REDUCE      = 1
)

type Task struct {
	lock      sync.Mutex
	filename  string
	state     int
	timestamp time.Time
}

func wait(task *Task) {
	time.Sleep(10 * time.Second)
	task.lock.Lock()
	if task.state == COMPLETE {
		fmt.Print(os.Stderr, " Task ", task.filename, " finished\n")
	} else {
		task.state = IDLE
		fmt.Print(os.Stderr, " Task ", task.filename, " failed\n")
	}
	task.lock.Unlock()
}

// give the asking worker a task if possible
func (c *Coordinator) HandleAsk(args *AskArgs, reply *AskReply) error {
	// log.Print("ask received")
	reply.Kind = "none"

	c.mu.Lock() // Protect shared state
	defer c.mu.Unlock()

	if c.map_remain != 0 {
		for i := range c.mtasks {
			task := &c.mtasks[i] // Pointer to the actual struct
			task.lock.Lock()
			defer task.lock.Unlock()
			if task.state == IDLE {
				task.state = IN_PROGRESS // Update task state
				reply.Kind = "map"
				reply.File = task.filename
				reply.NReduce = len(c.rtasks)
				reply.Index = i
				task.timestamp = time.Now()
				go wait(task)
				return nil
			}
		}
	} else {
		for i := range c.rtasks {
			task := &c.rtasks[i] // Pointer to the actual struct
			task.lock.Lock()
			defer task.lock.Unlock()
			if task.state == IDLE {
				task.state = IN_PROGRESS // Update task state
				reply.Kind = "reduce"
				reply.Splite = len(c.mtasks)
				reply.Index = i
				task.timestamp = time.Now()
				go wait(task)
				return nil
			}
		}
	}

	return nil
}

func (c *Coordinator) HandleResponse(args *ResponseArgs, reply *ResponseReply) error {
	now := time.Now()
	// log.Print("response received")

	// Validate task kind and index
	var task *Task
	if args.Kind == "map" {
		if args.Index < 0 || args.Index >= len(c.mtasks) {
			return fmt.Errorf("invalid map task index: %d", args.Index)
		}
		task = &c.mtasks[args.Index]
	} else if args.Kind == "reduce" {
		if args.Index < 0 || args.Index >= len(c.rtasks) {
			return fmt.Errorf("invalid reduce task index: %d", args.Index)
		}
		task = &c.rtasks[args.Index]
	} else {
		return fmt.Errorf("invalid task kind: %s", args.Kind)
	}

	// Check if the task is completed within the allowed time frame
	task.lock.Lock()
	defer task.lock.Unlock()
	if now.Before(task.timestamp.Add(10 * time.Second)) {
		task.state = COMPLETE
		log.Printf("Task %d of type %s marked as complete", args.Index, args.Kind)
	} else {
		log.Printf("Task %d of type %s timed out", args.Index, args.Kind)
		return nil // Do not count it as an error; the task will be retried
	}

	// Update the remaining task count
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Kind == "map" {
		c.map_remain--
		log.Printf("Remaining map tasks: %d", c.map_remain)
	} else if args.Kind == "reduce" {
		c.reduce_remain--
		log.Printf("Remaining reduce tasks: %d", c.reduce_remain)
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false

	c.mu.Lock()
	if c.reduce_remain == 0 {
		ret = true
	}
	c.mu.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Initialize map tasks
	c.mtasks = make([]Task, len(files))
	c.map_remain = len(files)
	for i, file := range files {
		c.mtasks[i] = Task{
			lock:     sync.Mutex{},
			filename: file,
			state:    IDLE,
		}
	}

	// Initialize reduce tasks
	c.rtasks = make([]Task, nReduce)
	c.reduce_remain = nReduce
	for i := 0; i < nReduce; i++ {
		c.rtasks[i] = Task{
			lock:  sync.Mutex{},
			state: IDLE,
		}
	}

	// Start RPC server
	go c.server()
	log.Println("Master: initialization completed")
	return &c
}
