package mr

import (
	"encoding/json"
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
	JobQueue  *JobQueue
	MailBox   CoorMailBox
	workerMap WorkerMap
	nReduce   int

	mapJobWaitGroup    sync.WaitGroup
	reduceJobWaitGroup sync.WaitGroup

	Phase Phase
}

type Phase int

const (
	PHASE_MAP Phase = iota
	PHASE_REDUCE
)

// WorkerMap stores every workers' jobs
type WorkerMap map[WorkerID]map[JobID]struct{}

func NewWorkerMap() WorkerMap {
	return WorkerMap{}
}

func debug(i interface{}) string {
	b, err := json.MarshalIndent(i, "", "\t")
	if err != nil {
		panic(err)
	}
	return string(b)
}

// AddJob add job to specific worker.
func (m WorkerMap) AddJob(j Job, workerID WorkerID) error {
	jobsForWorker, ok := m[workerID]
	if !ok {
		// it's new worker
		jobsForWorker = map[JobID]struct{}{}
	}
	_, ok = jobsForWorker[j.ID]
	if ok {
		return fmt.Errorf("job %v already exists", j.ID)
	}
	// Job Not Found
	jobsForWorker[j.ID] = struct{}{}
	m[workerID] = jobsForWorker
	return nil
}

func (m WorkerMap) FinishJob(workerID WorkerID, jobID JobID) error {
	var jobs map[JobID]struct{}
	var ok bool
	if jobs, ok = m[workerID]; !ok {
		return fmt.Errorf("worker not found")
	}
	if _, ok := jobs[jobID]; !ok {
		return fmt.Errorf("job not found")
	}
	delete(jobs, jobID)
	m[workerID] = jobs
	return nil
}

func (c *Coordinator) logCoordinator(format string, args ...interface{}) {
	log.Printf("Coordinator[]\t"+format, args...)
}

type JobQueue struct {
	coor *Coordinator
	ch   chan Job
}

func NewLocalJobQueue(capacity int, coor *Coordinator) *JobQueue {
	return &JobQueue{ch: make(chan Job, capacity), coor: coor}
}

func (j *JobQueue) Submit(job Job) error {
	j.ch <- job
	return nil
}

func (j *JobQueue) NumOfJobs() int {
	return len(j.ch)
}

func (j *JobQueue) GetJob() (Job, error) {
	job := <-j.ch
	return job, nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FinishJob(workerID WorkerID, jobID JobID) error {
	if err := c.workerMap.FinishJob(workerID, jobID); err != nil {
		return fmt.Errorf("failed on Coordinator.FinishJob")
	}
	// job type
	switch c.Phase {
	case PHASE_MAP:
		c.mapJobWaitGroup.Done()
	case PHASE_REDUCE:
		c.reduceJobWaitGroup.Done()
	}
	return nil
}

func (c *Coordinator) WordCount(args *WordCountArgs, reply *WordCountReply) error {
	c.Phase = PHASE_MAP
	log.Printf("in c.WordCount, args: %v\n", args)
	mapJobs := make([]Job, 0, len(args.FileNames))

	c.mapJobWaitGroup.Add(len(args.FileNames))

	for _, fileName := range args.FileNames {
		// assign job for every file
		job := NewJob(fileName, TYPE_MAP)
		if err := c.JobQueue.Submit(job); err != nil {
			return err
		}
		c.logCoordinator("length of jobs: %v", c.JobQueue.NumOfJobs())
		mapJobs = append(mapJobs, job)
	}

	c.WaitForMap()

	c.logCoordinator("all map jobs are done")

	c.Phase = PHASE_REDUCE

	c.reduceJobWaitGroup.Add(c.nReduce)

	for reduceNum := 0; reduceNum < c.nReduce; reduceNum++ {
		j := Job{
			ID:        NewJobID(),
			JobType:   TYPE_REDUCE,
			ReduceNum: reduceNum,
		}
		// reuse same job
		if err := c.JobQueue.Submit(j); err != nil {
			return err
		}
	}

	c.WaitForReduce()

	c.logCoordinator("all jobs are done")

	return nil
}

func (c *Coordinator) WaitForReduce() {
	c.reduceJobWaitGroup.Wait()
}

func (c *Coordinator) WaitForMap() {
	c.mapJobWaitGroup.Wait()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) Serve() {
	c.MailBox.Serve()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// FIXME: should handle this
	time.Sleep(10 * time.Second)
	return true
}

func (c *Coordinator) GetJobs(args GetJobsArgs, reply *GetJobsReply) error {
	// TODO: Take batch of jobs from jobQueue
	j, err := c.JobQueue.GetJob()
	if err != nil {
		reply.Jobs = nil
		reply.Err = fmt.Errorf("failed on c.JobQueue.GetJob: %v", err)
		return reply.Err
	}
	if err := c.workerMap.AddJob(j, args.ID); err != nil {
		reply.Jobs = nil
		reply.Err = fmt.Errorf("failed on c.workerMap.AddJob: %v", err)
		return reply.Err
	}
	c.logCoordinator("c.workerMap: %v", debug(c.workerMap))
	reply.Jobs = []Job{j}
	reply.Err = nil
	return nil
}

const DefaultJobQueueCap = 10

// FIXME: Coordinator should know nReduce in advance ?
func NewLocalCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{nReduce: nReduce}
	m := &localMailBox{coorService: c}
	c.MailBox = m
	c.workerMap = NewWorkerMap()
	c.JobQueue = NewLocalJobQueue(DefaultJobQueueCap, c)
	c.nReduce = nReduce
	c.Serve()
	return c
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func NewRPCCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{nReduce: nReduce}
	m := &RPCMailBox{coorService: c}
	c.MailBox = m
	c.workerMap = NewWorkerMap()
	c.JobQueue = NewLocalJobQueue(DefaultJobQueueCap, c)
	c.nReduce = nReduce
	return c
}

type CoorMailBox interface {
	Serve()
	GetJobs(id WorkerID) ([]Job, error)
	Done() bool
	FinishJob(workerID WorkerID, jobID JobID) error
	Example(args *ExampleArgs, reply *ExampleReply) error
}

type localMailBox struct {
	coorService *Coordinator
}

func (l *localMailBox) Serve() {
	return
}

func (l *localMailBox) GetJobs(workerID WorkerID) ([]Job, error) {
	args := GetJobsArgs{workerID}
	reply := &GetJobsReply{}
	l.coorService.GetJobs(args, reply)
	if reply.Err != nil {
		return nil, fmt.Errorf("failed on l.coorService.GetJobs: %v", reply.Err)
	}
	return reply.Jobs, nil
}

func (l *localMailBox) FinishJob(workerID WorkerID, jobID JobID) error {
	if err := l.coorService.FinishJob(workerID, jobID); err != nil {
		return fmt.Errorf("failed on l.coorService.FinishJob: %v", err)
	}
	return nil
}

func (l *localMailBox) Done() bool {
	return l.coorService.Done()
}

func (l *localMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type RPCMailBox struct {
	coorService *Coordinator
}

func (r *RPCMailBox) Serve() {
	rpc.Register(r.coorService)
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

func (r *RPCMailBox) GetJobs(workerID WorkerID) ([]Job, error) {
	// should do rpc call
	// declare an argument structure.
	args := GetJobsArgs{ID: workerID}

	// declare a reply structure.
	reply := GetJobsReply{}

	rpcName := "Coordinator.GetJobs"
	if err := call(rpcName, &args, &reply); err != nil {
		return nil, fmt.Errorf("failed on rpc call [%v]: %v", rpcName, err)
	}
	/*
		jobs, err := r.coorService.GetJobs(workerID)
		if err != nil {
			return nil, fmt.Errorf("failed on l.coorService.GetJobs: %v", err)
		}
	*/
	return reply.Jobs, nil
}

func (r *RPCMailBox) FinishJob(workerID WorkerID, jobID JobID) error {
	if err := r.coorService.FinishJob(workerID, jobID); err != nil {
		return fmt.Errorf("failed on l.coorService.FinishJob: %v", err)
	}
	return nil
}

func (r *RPCMailBox) Done() bool {
	// hang here forever
	// TODO: Handle this done check
	return r.coorService.Done()
}

func (r *RPCMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
