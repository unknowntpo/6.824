package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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

type ByKey []KeyValue

func (k ByKey) Len() int {
	return len(k)
}

func (k ByKey) Less(i, j int) bool {
	return k[i].Key < k[j].Key
}

func (k ByKey) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}

type keyIHash int
type KeyValuesMap map[keyIHash][]KeyValue

type groupByFn func(key string) keyIHash

// groupBy groups kvs by fn
func groupBy(kvs []KeyValue, fn groupByFn) KeyValuesMap {
	out := KeyValuesMap{}
	for _, kv := range kvs {
		hashKey := fn(kv.Key)
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
	// the reduce index
	// e.g. if worker get a job with JobType == TYPE_MAP,
	// and ReduceNum == 0, it has to grab all files with mr-<ihash>-0, and reduce them
	// to final result file `mr-out-0`
	ReduceNum int
}

type JobType int

const (
	TYPE_MAP JobType = iota
	TYPE_REDUCE
)

func (jt JobType) MarshalJSON() ([]byte, error) {
	switch jt {
	case TYPE_MAP:
		return []byte(`"TYPE_MAP"`), nil
	case TYPE_REDUCE:
		return []byte(`"TYPE_REDUCE"`), nil
	default:
		return nil, fmt.Errorf("unknown JobType: %d", jt)
	}
}

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

func NewLocalWorker(m CoorMailBox, mapFn MapFn, reduceFn ReduceFn, nReduce int) Worker {
	return &localWorker{
		ID:        NewWorkerID(),
		nReduce:   nReduce,
		coMailBox: m,
		mapFn:     mapFn,
		reduceFn:  reduceFn,
	}
}

type localWorker struct {
	ID        WorkerID
	nReduce   int
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
			l.logWorker("got jobs: %v", debug(jobs))
			go l.handleJobs(ctx, jobs, errChan)
		case err := <-errChan:
			l.logWorker("%v", err)
		case <-ctx.Done():
			return nil
		}
	}
}

func getWd() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return dir
}

func (l *localWorker) doReduce(j Job, kvs []KeyValue) error {
	l.logWorker("in doReduce for job %v", debug(j))
	oname := fmt.Sprintf("mr-out-%d", j.ReduceNum)
	dir := getWd()
	path := filepath.Join(dir, oname)
	ofile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	defer ofile.Close()
	if err != nil {
		return fmt.Errorf("failed to open output file [%v]: %v", oname, err)
	}

	l.logWorker("writing ofile %v", ofile.Name())

	// NOTE: Copied from src/main/mrsequential.go
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := l.reduceFn(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	return nil
}

func (l *localWorker) handleJobs(ctx context.Context, jobs []Job, errChan chan error) {
	dir := getWd()
	l.logWorker("wd: %v", dir)
	for _, j := range jobs {
		switch j.JobType {
		case TYPE_MAP:
			b, err := ioutil.ReadFile(j.FileName)
			if err != nil {
				// FIXME: multiple errors ?
				errChan <- fmt.Errorf("failed to read file [%v]: %v", j.FileName, err)
				return
			}

			kvs := l.mapFn(j.FileName, string(b))
			// intermediate file
			kvsMap := groupBy(kvs, func(key string) keyIHash {
				return ihash(key) % keyIHash(l.nReduce)
			})

			for keyIHash, kvs := range kvsMap {
				// format: map-<ihash(j.filename)>-<keyIHash>
				sort.Sort(ByKey(kvs))
				fileName := getIntermediateFileName(j.FileName, keyIHash)
				l.logWorker("writing file: %s", fileName)
				if err := writeKeyValuesToFile(fileName, kvs); err != nil {
					errChan <- fmt.Errorf("failed on writeKeyValuesToFile: %v", err)
					return
				}
			}
			l.logWorker("job [%v] is handled\n", debug(j))
		case TYPE_REDUCE:
			l.logWorker("Reduce job [%v] is found\n", debug(j))
			// Open mr-*-j.ReduceNum
			// mr-1291122704-4
			fileNames, err := getIntermediateFileNameByReduceNum(j.ReduceNum)
			if err != nil {
				errChan <- fmt.Errorf("failed on getIntermediateFileNameByReduceNum[%v]: %v", j.ReduceNum, err)
				return
			}

			kvs := []KeyValue{}

			l.logWorker("got fileNames: %v", fileNames)

			for _, f := range fileNames {
				l.logWorker("failed before readKeyValuesFromFile")
				_kvs, err := l.readKeyValuesFromFile(f)
				if err != nil {
					errChan <- fmt.Errorf("failed on readKeyValuesFromFile for %v: %v", f, err)
					return
				}
				kvs = append(kvs, _kvs...)
			}

			l.logWorker("job [%v] finish appending kvs: length: %v", debug(j), len(kvs))

			l.doReduce(j, kvs)
		}
		if err := l.coMailBox.FinishJob(l.ID, j.ID); err != nil {
			errChan <- fmt.Errorf("failed on l.coMailBox.FinishJob: %v", err)
		}
	}
}

func getIntermediateFileNameByReduceNum(reduceNum int) ([]string, error) {
	pattern := fmt.Sprintf("mr-(\\d+)-%d", reduceNum)
	root := "./"

	fileNames := []string{}

	// Find all files in the root directory
	files, err := ioutil.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("failed on ioutil.ReadDir: %v", err)
	}

	// Compile the regular expression pattern
	re := regexp.MustCompile(pattern)

	// Iterate over each file
	for _, file := range files {
		// Check if the file matches the pattern
		if re.MatchString(file.Name()) {
			fileNames = append(fileNames, file.Name())
		}
	}

	fmt.Println("files", fileNames)

	return fileNames, nil
}

func getIntermediateFileName(fileName string, keyIHash keyIHash) string {
	return fmt.Sprintf("mr-%v-%v", ihash(fileName), keyIHash)
}

func writeKeyValuesToFile(fileName string, kvs []KeyValue) error {
	dir := getWd()
	f, err := os.OpenFile(filepath.Join(dir, fileName), os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()
	if err != nil {
		return fmt.Errorf("failed on os.OpenFile: %v", err)
	}
	b, err := json.Marshal(KeyValues{kvs})
	if err != nil {
		return fmt.Errorf("failed on json.Marshal: %v", err)
	}
	if _, err := f.Write(b); err != nil {
		return fmt.Errorf("failed on f.Write: %v", err)
	}
	return nil
}

func (l *localWorker) readKeyValuesFromFile(fileName string) ([]KeyValue, error) {
	path := filepath.Join(getWd(), fileName)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed on os.OpenFile for %v: %v", path, err)
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed on ioutil.ReadAll for %v: %v", path, err)
	}

	out := KeyValues{KVS: make([]KeyValue, 0, 1000)}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("failed on json.Unmarshal for %v: %v", path, err)
	}

	return out.KVS, nil
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
