package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	JobQueue  *JobQueue
	MailBox   CoorMailBox
	workerMap WorkerMap
	nReduce   int
	// The number of unfinished jobs in Coordinator
	mapJobDoneChan    chan struct{}
	reduceJobDoneChan chan struct{}

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
		if _, ok := <-c.mapJobDoneChan; !ok {
			// All map jobs are complete
			close(c.mapJobDoneChan)
		}
	case PHASE_REDUCE:
		if _, ok := <-c.reduceJobDoneChan; !ok {
			// All map jobs are complete
			close(c.mapJobDoneChan)
		}
	}
	return nil
}

func (c *Coordinator) WordCount(args *WordCountArgs, reply *WordCountReply) error {
	//	go c.monitorJobs()
	//
	c.Phase = PHASE_MAP
	log.Printf("in c.WordCount, args: %v\n", args)
	mapJobs := make([]Job, 0, len(args.FileNames))
	for _, fileName := range args.FileNames {
		// assign job for every file
		job := NewJob(fileName, TYPE_MAP)
		if err := c.JobQueue.Submit(job); err != nil {
			return err
		}
		c.logCoordinator("length of jobs: %v", c.JobQueue.NumOfJobs())
		mapJobs = append(mapJobs, job)
	}

	// number of map jobs will be args.FileNames
	c.mapJobDoneChan = make(chan struct{}, len(args.FileNames))

	// Set jobDoneChan
	for i := 0; i < len(mapJobs); i++ {
		c.mapJobDoneChan <- struct{}{}
	}

	c.WaitForMap()

	c.logCoordinator("all map jobs are done")

	c.Phase = PHASE_REDUCE

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

	// TODO: extract setup of c.mapJobDoneChan to a function
	c.reduceJobDoneChan = make(chan struct{}, c.nReduce)
	for i := 0; i < len(mapJobs); i++ {
		c.reduceJobDoneChan <- struct{}{}
	}
	c.WaitForReduce()
	c.logCoordinator("all jobs are done")
	return nil
}

func (c *Coordinator) WaitForReduce() {
	// wait until map job finished
	<-c.reduceJobDoneChan
}

func (c *Coordinator) WaitForMap() {
	// wait until map job finished
	<-c.mapJobDoneChan
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) Serve() {
	c.MailBox.Serve()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.MailBox.Done()
}

func (c *Coordinator) GetJobs(id WorkerID) ([]Job, error) {
	// TODO: Take batch of jobs from jobQueue
	j, err := c.JobQueue.GetJob()
	if err != nil {
		return nil, fmt.Errorf("failed on c.JobQueue.GetJob: %v", err)
	}
	if err := c.workerMap.AddJob(j, id); err != nil {
		return nil, fmt.Errorf("failed on c.workerMap.AddJob: %v", err)
	}
	c.logCoordinator("c.workerMap: %v", debug(c.workerMap))
	return []Job{j}, nil
}

const DefaultJobQueueCap = 10

// FIXME: Coordinator should know nReduce in advance ?
func NewLocalCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{nReduce: nReduce}
	m := &localMailBox{coorService: c}
	c.MailBox = m
	c.workerMap = NewWorkerMap()
	c.JobQueue = NewLocalJobQueue(DefaultJobQueueCap, c)
	c.mapJobDoneChan = make(chan struct{})
	c.Serve()
	return c
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func NewRPCCoordinator(files []string, nReduce int) *Coordinator {
	m := &rpcMailBox{}
	c := Coordinator{MailBox: m}
	c.mapJobDoneChan = make(chan struct{})
	c.Serve()
	return &c
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
	jobs, err := l.coorService.GetJobs(workerID)
	if err != nil {
		return nil, fmt.Errorf("failed on l.coorService.GetJobs: %v", err)
	}
	return jobs, nil
}

func (l *localMailBox) FinishJob(workerID WorkerID, jobID JobID) error {
	// Find job in wokrerMap
	l.coorService.FinishJob(workerID, jobID)
	return nil
}

func (l *localMailBox) Done() bool {
	// hang here forever
	// TODO: Handle this done check
	return true
}

func (l *localMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type rpcMailBox struct {
}

func (r *rpcMailBox) Serve() {
	rpc.Register(r)
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

func (r *rpcMailBox) GetJobs(workerID WorkerID) ([]Job, error) {
	return []Job{}, nil
}

func (r *rpcMailBox) FinishJob(workerID WorkerID, jobID JobID) error {
	return nil
}

func (r *rpcMailBox) Done() bool {
	// hang here forever
	// TODO: Handle this done check
	return true
}

func (r *rpcMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
