package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"sort"
	"time"

	// "io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Reduce functions take a slice of KeyValue and return a string.
type ReduceFunc func(string, []string) string

// MapReduce takes a map function and a reduce function
// and applies them to the input.
func MapReduce(mapf func(string, string) []KeyValue,
	reducef ReduceFunc, input []string) {

	// Your code here.

}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func performMap(mapf func(string, string) []KeyValue, filename string, nReduce int, index int) bool {
	kvall := make([][]KeyValue, nReduce)
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: can not open %v\n", time.Now().String(), filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: can not read %v\n", time.Now().String(), filename)
		return false
	}
	file.Close()

	map_res := mapf(filename, string(content))

	// map result are mapped into `nReduce` bucket
	for _, kv := range map_res {
		index := ihash(kv.Key) % nReduce
		kvall[index] = append(kvall[index], kv)
	}

	// write key-value to different json files
	for i, kva := range kvall {
		// implement atomical write by two-phase trick: write to a temporary file and rename it
		oldname := fmt.Sprintf("temp_inter_%d_%d.json", index, i)
		tempfile, err := os.OpenFile(oldname, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: map can not open temp file %v\n", time.Now().String(), oldname)
			return false
		}
		defer os.Remove(oldname)

		enc := json.NewEncoder(tempfile)
		for _, kv := range kva {
			if err := enc.Encode(&kv); err != nil {
				fmt.Fprintf(os.Stderr, "%s Worker: map can not write to temp file %v\n", time.Now().String(), oldname)
				return false
			}
		}

		newname := fmt.Sprintf("inter_%d_%d.json", index, i)
		if err := os.Rename(oldname, newname); err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: map can not rename temp file %v\n", time.Now().String(), oldname)
			return false
		}
	}
	return true
}
func performReduce(reducef func(string, []string) string, splite int, index int) bool {
	var kva []KeyValue
	for i := 0; i < splite; i++ {
		filename := fmt.Sprintf("inter_%d_%d.json", i, index)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: can not read intermidiate file %v\n", time.Now().String(), filename)
			return false
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	// two-phase trick to implement atomical write
	oldname := fmt.Sprintf("temp-mr-out-%d", index)
	newname := fmt.Sprintf("mr-out-%d", index)

	tempfile, err := os.OpenFile(oldname, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: reduce can not open temp file %v\n", time.Now().String(), oldname)
		return false
	}
	defer os.Remove(oldname)

	// reduce on values that have the same key
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	if err := os.Rename(oldname, newname); err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: reduce can not rename temp file %v\n", time.Now().String(), oldname)
		return false
	}

	return true
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.Print("Worker started")

	for {
		// Request a task from the coordinator
		// log.Print("Worker: requesting a task from the coordinator")
		args := AskArgs{}
		reply := AskReply{}
		if !call("Coordinator.HandleAsk", &args, &reply) {
			log.Print("Worker: call to Coordinator.HandleAsk failed, exiting.")
			os.Exit(1)
		}

		// No task available; wait and retry
		if reply.Kind == "none" {
			time.Sleep(1 * time.Second)
			continue
		}

		// Prepare to send a response after task completion
		responseArgs := ResponseArgs{Kind: reply.Kind, Index: reply.Index}
		responseReply := ResponseReply{}
		// Execute the task based on its kind
		switch reply.Kind {
		case "map":
			if performMap(mapf, reply.File, reply.NReduce, reply.Index) {
				log.Printf("Worker: map task %v completed successfully", reply.Index)
			} else {
				log.Printf("Worker: map task %v failed", reply.Index)
				continue
			}
		case "reduce":
			if performReduce(reducef, reply.Splite, reply.Index) {
				log.Printf("Worker: reduce task %v completed successfully", reply.Index)
			} else {
				log.Printf("Worker: reduce task %v failed", reply.Index)
				continue
			}
		default:
			log.Printf("Worker: unknown task kind %v", reply.Kind)
			continue
		}

		// Notify the coordinator about task completion
		if !call("Coordinator.HandleResponse", &responseArgs, &responseReply) {
			log.Printf("Worker: call to Coordinator.HandleResponse failed, exiting.")
			os.Exit(1)
		}
		// Short delay before requesting the next task
		time.Sleep(1 * time.Second)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("error calling", rpcname, ":", err)
	return false
}
