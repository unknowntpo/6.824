package mr

import (
	"encoding/json"
	"errors"
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
	workerMap *WorkerMap
	// Shouldn't be changed after init
	nReduce int

	mapJobWaitGroup    sync.WaitGroup
	reduceJobWaitGroup sync.WaitGroup

	phase   Phase
	phaseMu sync.RWMutex
}

type Phase int32

const (
	PHASE_INIT Phase = iota
	PHASE_MAP
	PHASE_REDUCE
	PHASE_DONE
)

// WorkerMap stores every workers' jobs

type WorkerMap struct {
	mu sync.RWMutex
	m  map[WorkerID]map[JobID]struct{}
}

func NewWorkerMap() *WorkerMap {
	m := &WorkerMap{}
	m.m = map[WorkerID]map[JobID]struct{}{}
	return m
}

func (w *WorkerMap) NumOfWorker() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	fmt.Println("num of worker", len(w.m))
	return len(w.m)
}

func (w *WorkerMap) RemoveWorker(workerID WorkerID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.m, workerID)
}

func debug(i interface{}) string {
	b, err := json.MarshalIndent(i, "", "\t")
	if err != nil {
		panic(err)
	}
	return string(b)
}

// AddJob add job to specific worker.
func (m *WorkerMap) AddJob(j Job, workerID WorkerID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	jobsForWorker, ok := m.m[workerID]
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
	m.m[workerID] = jobsForWorker
	return nil
}

func (m *WorkerMap) FinishJob(workerID WorkerID, jobID JobID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var jobs map[JobID]struct{}
	var ok bool
	if jobs, ok = m.m[workerID]; !ok {
		return fmt.Errorf("worker not found")
	}
	if _, ok := jobs[jobID]; !ok {
		return fmt.Errorf("job not found")
	}
	delete(jobs, jobID)
	m.m[workerID] = jobs
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

func (c *Coordinator) FinishJob(args *FinishJobsArgs, reply *FinishJobsReply) error {
	if err := c.workerMap.FinishJob(args.WorkerID, args.JobID); err != nil {
		return fmt.Errorf("failed on Coordinator.FinishJob for workerID [%v], jobID: [%v]: %v", args.WorkerID, args.JobID, err)
	}
	// job type
	switch c.phase {
	case PHASE_MAP:
		c.mapJobWaitGroup.Done()
	case PHASE_REDUCE:
		c.reduceJobWaitGroup.Done()
	}
	return nil
}

func (c *Coordinator) WordCount(args *WordCountArgs, reply *WordCountReply) error {
	c.ChangePhase(PHASE_MAP)

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

	c.ChangePhase(PHASE_REDUCE)

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

	c.ChangePhase(PHASE_DONE)

	c.logCoordinator("c.Phase: %v", c.phase)

	c.logCoordinator("all jobs are done")

	// Wait for all worker to die
	for !c.Done() {
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func (c *Coordinator) ChangePhase(desiredPhase Phase) {
	c.phaseMu.Lock()
	defer c.phaseMu.Unlock()
	if c.phase != desiredPhase-1 {
		panic("broken state")
	}
	c.phase = desiredPhase
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

func (c *Coordinator) PhaseIs(p Phase) bool {
	c.phaseMu.RLock()
	defer c.phaseMu.RUnlock()
	return c.phase == p
}

// main/mrcoordinator.go calls Done() periodically to see if all jobs are done.
func (c *Coordinator) Done() bool {
	c.phaseMu.RLock()
	defer c.phaseMu.RUnlock()
	return c.PhaseIs(PHASE_DONE) && c.workerMap.NumOfWorker() == 0
}

var (
	ErrDone = errors.New("all jobs are done")
)

func (c *Coordinator) GetJobs(args *GetJobsArgs, reply *GetJobsReply) error {
	if c.PhaseIs(PHASE_DONE) {
		c.logCoordinator("in coor: Jobs are done")
		c.workerMap.RemoveWorker(args.ID)
		reply.Err = ErrDone
		return ErrDone
	}
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
	args := &GetJobsArgs{workerID}
	reply := &GetJobsReply{}
	defer l.coorService.logCoordinator("reply: %v", debug(reply))
	l.coorService.GetJobs(args, reply)
	if reply.Err != nil {
		switch {
		case reply.Err == ErrDone:
			return nil, reply.Err
		default:
			return nil, fmt.Errorf("failed on l.coorService.GetJobs: %v", reply.Err)
		}
	}
	return reply.Jobs, nil
}

func (l *localMailBox) FinishJob(workerID WorkerID, jobID JobID) error {
	args := FinishJobsArgs{WorkerID: workerID, JobID: jobID}
	reply := FinishJobsReply{}
	if err := l.coorService.FinishJob(&args, &reply); err != nil {
		return fmt.Errorf("failed on l.coorService.FinishJob: %v", err)
	}
	return nil
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
	args := GetJobsArgs{ID: workerID}
	reply := GetJobsReply{}
	rpcName := "Coordinator.GetJobs"
	if err := call(rpcName, &args, &reply); err != nil {
		switch {
		case reply.Err == ErrDone:
			return nil, reply.Err
		default:
			return nil, fmt.Errorf("failed on rpc call [%v]: %v", rpcName, err)
		}
	}
	return reply.Jobs, nil
}

func (r *RPCMailBox) FinishJob(workerID WorkerID, jobID JobID) error {
	args := FinishJobsArgs{WorkerID: workerID, JobID: jobID}
	reply := FinishJobsReply{}
	rpcName := "Coordinator.FinishJob"
	if err := call(rpcName, &args, &reply); err != nil {
		return fmt.Errorf("failed on rpc call [%v]: %v", rpcName, err)
	}
	return nil
}

func (r *RPCMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
