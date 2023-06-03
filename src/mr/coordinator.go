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
	JobQueue       *JobQueue
	MailBox        CoorMailBox
	workerMap      WorkerMap
	nReduce        int
	mapDoneChan    chan struct{}
	reduceDoneChan chan struct{}
}

// WorkerMap stores every workers' jobs
type WorkerMap map[WorkerID][]Job

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
	m[workerID] = append(m[workerID], j)
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
	j.coor.logCoordinator("try to submit jobs: %v", job)
	j.ch <- job
	j.coor.logCoordinator("job Submited: %v", job)
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

//func (c *Coordinator) MonitorJob(args *WordCountArgs, reply *WordCountReply) error {

func (c *Coordinator) WordCount(args *WordCountArgs, reply *WordCountReply) error {
	//	go c.monitorJobs()
	//
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

	c.WaitForMap()

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
	<-c.reduceDoneChan
}

func (c *Coordinator) WaitForMap() {
	// wait until map job finished
	<-c.mapDoneChan
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
	c.mapDoneChan = make(chan struct{})
	c.Serve()
	return c
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func NewRPCCoordinator(files []string, nReduce int) *Coordinator {
	m := &rpcMailBox{}
	c := Coordinator{MailBox: m}
	c.mapDoneChan = make(chan struct{})
	c.Serve()
	return &c
}

type CoorMailBox interface {
	Serve()
	GetJobs(id WorkerID) ([]Job, error)
	Done() bool
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

func (r *rpcMailBox) Done() bool {
	// hang here forever
	// TODO: Handle this done check
	return true
}

func (r *rpcMailBox) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
