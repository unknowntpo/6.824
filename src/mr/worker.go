package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

	uuid "github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	KVS []KeyValue
}

type keyIHash int
type KeyValuesMap map[keyIHash][]KeyValue

func GroupByKeyIHash(kvs []KeyValue) KeyValuesMap {
	nReduce := 10
	out := KeyValuesMap{}
	for _, kv := range kvs {
		hashKey := ihash(kv.Key) % keyIHash(nReduce)
		out[hashKey] = append(out[hashKey], kv)
	}
	return out
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) keyIHash {
	h := fnv.New32a()
	h.Write([]byte(key))
	return keyIHash(h.Sum32() & 0x7fffffff)
}

// See paper for the def of map, reduce func
// https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf
// key: doc name, value: document contents
type MapFn func(key string, value string) []KeyValue

// key: a word, values: a list of counts
type ReduceFn func(key string, values []string) string

// main/mrworker.go calls this function.
func Work(
	mapf MapFn,
	reducef ReduceFn,
) {

	// init new worker
	// // call example

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample()

}

type JobID string

func NewJobID() JobID {
	return JobID(uuid.Must(uuid.NewRandom()).String())
}

type Job struct {
	ID       JobID
	FileName string
	JobType  JobType
}

type JobType int

const (
	TYPE_MAP JobType = iota
	TYPE_REDUCE
)

func NewJob(fileName string, jobType JobType) Job {
	return Job{ID: NewJobID(), FileName: fileName, JobType: jobType}
}

type Worker interface {
	IsHealthy() bool
	Serve(ctx context.Context) error
	Shutdown()
}

type WorkerID string

func NewWorkerID() WorkerID {
	return WorkerID(uuid.Must(uuid.NewRandom()).String())
}

func NewLocalWorker(m CoorMailBox, mapFn MapFn, reduceFn ReduceFn) Worker {
	return &localWorker{
		ID:        NewWorkerID(),
		coMailBox: m,
		mapFn:     mapFn,
		reduceFn:  reduceFn,
	}
}

type localWorker struct {
	ID        WorkerID
	coMailBox CoorMailBox
	mapFn     MapFn
	reduceFn  ReduceFn
}

func (l *localWorker) IsHealthy() bool { return true }
func (l *localWorker) Serve(ctx context.Context) error {
	timer := time.NewTicker(300 * time.Millisecond)
	errChan := make(chan error, 30)
	for {
		select {
		case <-timer.C:
			jobs, err := l.coMailBox.GetJobs(l.ID)
			if err != nil {
				log.Println(err)
			}
			go l.handleJobs(ctx, jobs, errChan)
		case err := <-errChan:
			l.logWorker("%v", err)
		case <-ctx.Done():
			return nil
		}
	}
}

func (l *localWorker) handleJobs(ctx context.Context, jobs []Job, errChan chan error) {
	for _, j := range jobs {
		l.logWorker("job [%v] is handled\n", j)
		b, err := ioutil.ReadFile(j.FileName)
		if err != nil {
			// FIXME: multiple errors ?
			errChan <- fmt.Errorf("failed on ioutil.ReadFile: %v", err)
		}
		switch j.JobType {
		case TYPE_MAP:
			kvs := l.mapFn(j.FileName, string(b))
			// intermediate file
			kvsMap := GroupByKeyIHash(kvs)
			// l.logWorker(debug(kvsMap))
			// TODO: Reduce them to nReduce

			for keyIHash, kvs := range kvsMap {
				// format: map-<ihash(j.filename)>-<keyIHash>
				fileName := getIntermediateFileName(j.FileName, keyIHash)
				if err := writeKeyValuesToFile(fileName, kvs); err != nil {
					errChan <- fmt.Errorf("failed on writeKeyValuesToFile: %v", err)
				}
			}

		case TYPE_REDUCE:
			// open old intermediate file
			/*
				fileName := getIntermediateFileName()
				kvs, err := readKeyValuesFromFile(fileName)
				if err != nil {
					errChan <- fmt.Errorf("failed on writeKeyValuesToFile: %v", err)
				}
				_ = kvs
				// type ReduceFn func(key string, values []string) string
			*/
		}
	}
}

func getIntermediateFileName(fileName string, keyIHash keyIHash) string {
	return fmt.Sprintf("mr-%v-%v", ihash(fileName), keyIHash)
}

func writeKeyValuesToFile(fileName string, kvs []KeyValue) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			switch err {
			case io.EOF:
				break
			default:
				return err
			}
		}
	}
	return nil
}

func readKeyValuesFromFile(fileName string) ([]KeyValue, error) {
	f, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(f)
	out := make([]KeyValue, 0, 1000)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			switch err {
			case io.EOF:
				break
			default:
				return nil, err
			}
		}
		out = append(out, kv)
	}
}

func (l *localWorker) Shutdown() { return }

type rpcWorker struct{}

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

	fmt.Println(err)
	return false
}

func (l *localWorker) logWorker(format string, args ...interface{}) {
	log.Printf(fmt.Sprintf("WORKER[%v]\t", l.ID)+format, args...)
}
