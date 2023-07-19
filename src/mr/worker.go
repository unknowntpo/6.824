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
	"strings"
	"sync/atomic"
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

type ReqID string

func NewReqID() ReqID {
	return ReqID(uuid.Must(uuid.NewRandom()).String())
}

type WorkerID string

func NewWorkerID() WorkerID {
	return WorkerID(uuid.Must(uuid.NewRandom()).String())
}

func NewWorker(
	coorMailBox CoorMailBox,
	mapFn MapFn,
	reduceFn ReduceFn,
	nReduce int,
	workDir string,
) *Worker {
	return &Worker{
		ID:        NewWorkerID(),
		nReduce:   nReduce,
		coMailBox: coorMailBox,
		mapFn:     mapFn,
		reduceFn:  reduceFn,
		workDir:   workDir,
	}
}

func NewRPCWorker(
	co *Coordinator,
	mapFn MapFn,
	reduceFn ReduceFn,
	nReduce int,
	workDir string,
) *Worker {
	mailBox := &RPCMailBox{coorService: co}
	return &Worker{
		ID:        NewWorkerID(),
		nReduce:   nReduce,
		coMailBox: mailBox,
		mapFn:     mapFn,
		reduceFn:  reduceFn,
		workDir:   workDir,
	}
}

type Worker struct {
	ID        WorkerID
	nReduce   int
	coMailBox CoorMailBox
	mapFn     MapFn
	reduceFn  ReduceFn
	workDir   string
	done      atomic.Bool
}

func (l *Worker) HeartBeat() {
	timer := time.NewTicker(1 * time.Second)
	for range timer.C {
		l.logWorker("heart is beating")
	}
}

func (l *Worker) IsHealthy() bool { return true }
func (l *Worker) Serve(ctx context.Context) error {
	// go l.HeartBeat()
	defer l.logWorker("worker Exit Serve")
	heartBeatTimer := time.NewTicker(1 * time.Second)

	//	deadTimer := time.NewTicker(6 * time.Second)
	timer := time.NewTicker(1 * time.Microsecond)
	errChan := make(chan error, 30)

LOOP:
	for {
		select {
		case <-heartBeatTimer.C:
			if err := l.coMailBox.MarkHealthy(l.ID); err != nil {
				switch {
				case err == ErrDone:
					goto DONE
				default:
					errChan <- err
				}
			}
		case <-timer.C:
			l.logWorker("try to call get jobs")
			jobs, err := l.coMailBox.GetJobs(l.ID)
			if err != nil {
				switch {
				case strings.Contains(err.Error(), ErrDone.Error()):
					// All jobs are done, shutdown worker
					l.logWorker("worker receive ErrDone")
					goto DONE
				case strings.Contains(err.Error(), ErrNoJob.Error()):
					l.logWorker("no job")
					panic("p")
					goto LOOP
				default:
					errChan <- err
					goto LOOP
				}
			}

			if err := l.handleJobs(ctx, jobs); err != nil {
				switch {
				case err == ErrDone:
					goto DONE
				default:
					l.logWorker("%v", err)
					goto LOOP
				}
			}
		case err := <-errChan:
			l.logWorker("error chan got something %v", err)
		case <-ctx.Done():
			l.logWorker("ctx.Done")
			return nil
		}
	}

DONE:
	l.complete()
	return nil
}

func (l *Worker) complete() {
	l.done.CompareAndSwap(false, true)
	l.logWorker("all worker jobs are done")
}

func (l *Worker) Done() bool {
	return l.done.Load()
}

func (l *Worker) doReduce(j Job, kvs []KeyValue) error {
	oname := fmt.Sprintf("mr-out-%d", j.ReduceNum)
	path := filepath.Join(l.workDir, oname)
	ofile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	defer ofile.Close()
	if err != nil {
		return fmt.Errorf("failed to open output file [%v]: %v", oname, err)
	}

	sort.Sort(ByKey(kvs))

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

func (l *Worker) handleJobs(ctx context.Context, jobs []Job) error {
	l.logWorker("inside handleJobs for", jobs)
	for _, j := range jobs {
		switch j.JobType {
		case TYPE_MAP:
			b, err := ioutil.ReadFile(j.FileName)
			if err != nil {
				// FIXME: multiple errors ?
				return fmt.Errorf("failed to read file [%v]: %v", j.FileName, err)
			}

			kvs := l.mapFn(j.FileName, string(b))
			// intermediate file
			kvsMap := groupBy(kvs, func(key string) keyIHash {
				return ihash(key) % keyIHash(l.nReduce)
			})

			for keyIHash, kvs := range kvsMap {
				// format: map-<ihash(j.filename)>-<keyIHash>
				fileName := filepath.Join(l.workDir, getIntermediateFileName(j.FileName, keyIHash))
				if err := l.writeKeyValuesToFile(fileName, kvs); err != nil {
					return fmt.Errorf("failed on writeKeyValuesToFile: %v", err)
				}
			}
		case TYPE_REDUCE:
			// Open mr-*-j.ReduceNum
			// mr-1291122704-4
			fileNames, err := l.getIntermediateFileNameByReduceNum(j.ReduceNum)
			if err != nil {
				return fmt.Errorf("failed on getIntermediateFileNameByReduceNum[%v]: %v", j.ReduceNum, err)
			}

			kvs := []KeyValue{}

			for _, f := range fileNames {
				_kvs, err := l.readKeyValuesFromFile(f)
				if err != nil {
					return fmt.Errorf("failed on readKeyValuesFromFile for %v: %v", f, err)
				}
				kvs = append(kvs, _kvs...)
			}

			if err := l.doReduce(j, kvs); err != nil {
				return fmt.Errorf("failed on l.doReduce for %v: %v", j, err)
			}
		}
		l.logWorker("try to call FinishJob for job [%v]", j.ID)
		if err := l.coMailBox.FinishJob(l.ID, j.ID); err != nil {
			switch {
			case strings.Contains(err.Error(), ErrDone.Error()):
				return ErrDone
			default:
				return fmt.Errorf("failed on l.coMailBox.FinishJob: %v", err)
			}
		}
		l.logWorker("end of worker.handleJobs")
	}
	return nil
}

func (l *Worker) getIntermediateFileNameByReduceNum(reduceNum int) ([]string, error) {
	pattern := fmt.Sprintf("mr-(\\d+)-%d", reduceNum)
	root := l.workDir

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

	return fileNames, nil
}

func getIntermediateFileName(fileName string, keyIHash keyIHash) string {
	return fmt.Sprintf("mr-%v-%v", ihash(fileName), keyIHash)
}

func (l *Worker) writeKeyValuesToFile(fileName string, kvs []KeyValue) error {
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
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

func (l *Worker) readKeyValuesFromFile(fileName string) ([]KeyValue, error) {
	path := filepath.Join(l.workDir, fileName)
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
	if err := call("Coordinator.Example", &args, &reply); err != nil {
		// reply.Y should be 100.
		fmt.Printf("call failed!\n")
	} else {
		fmt.Printf(" reply.Y: %v\n", reply.Y)
	}
}

const RETRY_COUNT = 10

func dialWithRetry(retryCnt int) (*rpc.Client, error) {
	sockname := coordinatorSock()
	for {
		c, err := rpc.DialHTTP("unix", sockname)
		defer func() {
			if c != nil {
				c.Close()
			}
		}()
		if err != nil {
			switch {
			case strings.Contains(err.Error(), "connection refused"):
				if retryCnt > 0 {
					time.Sleep(10 * time.Millisecond)
					retryCnt--
					continue
				} else {
					// Treat it as done
					return nil, ErrDone
				}
			default:
				return nil, fmt.Errorf("dialing: %v", err)
			}
		}
		return c, nil
	}
}

func callWithRetry(rpcname string, args interface{}, reply interface{}) error {
	retryCnt := 10
	for {
		if err := call(rpcname, args, reply); err != nil {
			switch {
			case strings.Contains(err.Error(), "connection refused"):
			case strings.Contains(err.Error(), rpc.ErrShutdown.Error()):
				if retryCnt > 0 {
					time.Sleep(10 * time.Millisecond)
					retryCnt--
					continue
				} else {
					// Treat it as done
					return ErrDone
				}
			case err == ErrNoJob:
				return ErrNoJob
			default:
				return fmt.Errorf("dialing: %v", err)
			}
		} else {
			return nil
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return fmt.Errorf("dialing: %v", err)
	}
	if err := c.Call(rpcname, args, reply); err != nil {
		switch {
		case err == ErrDone:
			return ErrDone
		case err == ErrNoJob:
			return ErrNoJob
		default:
			return fmt.Errorf("failed on rpc.Client.Call: %v", err)
		}
	}
	return nil
}

func (l *Worker) logWorker(format string, args ...interface{}) {
	log.Printf(fmt.Sprintf("WORKER[%v]\t", l.ID)+format, args...)
}
